use std::collections::BTreeMap;

use thiserror::Error;

pub type ReplicaId = u16;
pub type LeaderEpoch = u64;
pub type LogIndex = u64;
pub type LogHash = [u8; 32];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SyncWatermark {
    pub leader_epoch: LeaderEpoch,
    pub committed_index: LogIndex,
    pub applied_index: LogIndex,
    pub last_applied_hash: LogHash,
}

impl SyncWatermark {
    pub fn is_fresh(&self) -> bool {
        self.committed_index == self.applied_index
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaReadiness {
    NotReady,
    TrafficDraining,
    FreshReadReplica,
    ActiveWriter,
    Draining,
    CleanShutdown,
    Stale,
    Diverged,
    OperatorRecoveryRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityPolicy {
    LocalOnly,
    RequirePassiveAcks { min_acks: u16 },
}

impl DurabilityPolicy {
    pub fn is_satisfied_by(self, ack_count: u16) -> bool {
        match self {
            Self::LocalOnly => true,
            Self::RequirePassiveAcks { min_acks } => ack_count >= min_acks,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitProof {
    pub durable_ack_bitmap: u64,
    pub durable_ack_count: u16,
}

impl CommitProof {
    pub fn local_only() -> Self {
        Self {
            durable_ack_bitmap: 0,
            durable_ack_count: 0,
        }
    }

    pub fn from_ack_bitmap(bitmap: u64) -> Self {
        let durable_ack_count = bitmap.count_ones().min(u16::MAX as u32) as u16;
        Self {
            durable_ack_bitmap: bitmap,
            durable_ack_count,
        }
    }

    /// Ack count derived from the bitmap, which is the only field the
    /// durability policy trusts. A peer can ship a proof with a bitmap-count
    /// mismatch (intentionally or by bug); durability checks must use this
    /// instead of the advisory `durable_ack_count` field.
    pub fn effective_ack_count(&self) -> u16 {
        self.durable_ack_bitmap
            .count_ones()
            .min(u16::MAX as u32) as u16
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedRecord {
    pub leader_epoch: LeaderEpoch,
    pub index: LogIndex,
    pub previous_hash: LogHash,
    pub record_hash: LogHash,
    pub writer_replica_id: ReplicaId,
    pub payload: Vec<u8>,
    pub proof: CommitProof,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommittedBatch {
    pub records: Vec<CommittedRecord>,
}

impl CommittedBatch {
    pub fn empty() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn first_index(&self) -> Option<LogIndex> {
        self.records.first().map(|record| record.index)
    }

    pub fn last_index(&self) -> Option<LogIndex> {
        self.records.last().map(|record| record.index)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedBatch {
    pub first_index: LogIndex,
    pub last_index: LogIndex,
    pub record_count: usize,
    pub target_watermark: SyncWatermark,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeDecision {
    AlreadyCurrent,
    FastForward {
        records_to_apply: usize,
    },
    QuarantineUncommittedSuffixThenFastForward {
        quarantine_from_index: LogIndex,
        records_to_apply: usize,
    },
    RejectDivergedCommittedLineage {
        local_index: LogIndex,
        remote_index: LogIndex,
    },
    RestoreFromSnapshot {
        trusted_index: LogIndex,
    },
}

pub trait ReplicatedLmdbDomain {
    type Error;

    fn validate_record(&self, record: &CommittedRecord) -> Result<(), Self::Error>;

    fn validate_payload_chain(
        &self,
        current: SyncWatermark,
        records: &[CommittedRecord],
    ) -> Result<(), Self::Error>;

    fn classify_divergence(
        &self,
        local: SyncWatermark,
        remote: SyncWatermark,
        local_suffix: &[CommittedRecord],
        remote_suffix: &[CommittedRecord],
    ) -> Result<MergeDecision, Self::Error>;

    fn apply_committed_batch(
        &mut self,
        records: &[CommittedRecord],
    ) -> Result<SyncWatermark, Self::Error>;
}

pub fn validate_committed_batch(
    current: SyncWatermark,
    batch: &CommittedBatch,
    durability_policy: DurabilityPolicy,
) -> Result<ValidatedBatch, ReplicationError> {
    let Some(first) = batch.records.first() else {
        return Err(ReplicationError::EmptyBatch);
    };
    if current.applied_index != current.committed_index {
        return Err(ReplicationError::StaleLocalWatermark {
            committed_index: current.committed_index,
            applied_index: current.applied_index,
        });
    }
    if first.index != current.committed_index.saturating_add(1) {
        return Err(ReplicationError::IndexGap {
            expected: current.committed_index.saturating_add(1),
            actual: first.index,
        });
    }
    if first.previous_hash != current.last_applied_hash {
        return Err(ReplicationError::PreviousHashMismatch { index: first.index });
    }

    let mut expected_index = first.index;
    let mut previous_hash = current.last_applied_hash;
    let mut leader_epoch = first.leader_epoch;
    for record in &batch.records {
        if record.index != expected_index {
            return Err(ReplicationError::IndexGap {
                expected: expected_index,
                actual: record.index,
            });
        }
        if record.leader_epoch < leader_epoch {
            return Err(ReplicationError::LeaderEpochRegression {
                previous: leader_epoch,
                actual: record.leader_epoch,
            });
        }
        leader_epoch = record.leader_epoch;
        if record.previous_hash != previous_hash {
            return Err(ReplicationError::PreviousHashMismatch {
                index: record.index,
            });
        }
        let effective_acks = record.proof.effective_ack_count();
        if !durability_policy.is_satisfied_by(effective_acks) {
            return Err(ReplicationError::InsufficientDurability {
                index: record.index,
                required_acks: match durability_policy {
                    DurabilityPolicy::LocalOnly => 0,
                    DurabilityPolicy::RequirePassiveAcks { min_acks } => min_acks,
                },
                actual_acks: effective_acks,
            });
        }
        previous_hash = record.record_hash;
        expected_index = expected_index.saturating_add(1);
    }

    let last = batch
        .records
        .last()
        .expect("non-empty batch already checked");
    Ok(ValidatedBatch {
        first_index: first.index,
        last_index: last.index,
        record_count: batch.records.len(),
        target_watermark: SyncWatermark {
            leader_epoch: last.leader_epoch,
            committed_index: last.index,
            applied_index: last.index,
            last_applied_hash: last.record_hash,
        },
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PassiveReplicaState {
    pub replica_id: ReplicaId,
    pub watermark: SyncWatermark,
    pub readiness: ReplicaReadiness,
}

impl PassiveReplicaState {
    pub fn new(replica_id: ReplicaId, watermark: SyncWatermark) -> Self {
        Self {
            replica_id,
            watermark,
            readiness: ReplicaReadiness::NotReady,
        }
    }

    pub fn apply_batch<D>(
        &mut self,
        domain: &mut D,
        batch: &CommittedBatch,
        durability_policy: DurabilityPolicy,
    ) -> Result<ValidatedBatch, ReplicationError>
    where
        D: ReplicatedLmdbDomain,
        D::Error: std::fmt::Display,
    {
        let plan = validate_committed_batch(self.watermark, batch, durability_policy)?;
        domain
            .validate_payload_chain(self.watermark, batch.records.as_slice())
            .map_err(|err| ReplicationError::DomainValidation(err.to_string()))?;
        let applied = domain
            .apply_committed_batch(batch.records.as_slice())
            .map_err(|err| ReplicationError::DomainApply(err.to_string()))?;
        if applied != plan.target_watermark {
            return Err(ReplicationError::AppliedWatermarkMismatch {
                expected: plan.target_watermark,
                actual: applied,
            });
        }
        self.watermark = applied;
        self.readiness = ReplicaReadiness::FreshReadReplica;
        Ok(plan)
    }

    pub fn mark_diverged(&mut self) {
        self.readiness = ReplicaReadiness::Diverged;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveReplicatorState {
    pub replica_id: ReplicaId,
    pub leader_epoch: LeaderEpoch,
    pub watermark: SyncWatermark,
    pub durability_policy: DurabilityPolicy,
    pub readiness: ReplicaReadiness,
    in_flight_commits: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrafficDrainState {
    Serving,
    NewRequestsBlocked,
    RoutedAway { replacement_replica_id: ReplicaId },
}

pub trait TrafficDrain {
    type Error;

    fn block_new_requests(&mut self, replica_id: ReplicaId) -> Result<(), Self::Error>;

    fn route_to_replacement(
        &mut self,
        draining_replica_id: ReplicaId,
        replacement_replica_id: ReplicaId,
    ) -> Result<(), Self::Error>;

    fn verify_routed_away(
        &self,
        draining_replica_id: ReplicaId,
    ) -> Result<Option<ReplicaId>, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafeShutdownPlan {
    pub draining_replica_id: ReplicaId,
    pub replacement_replica_id: ReplicaId,
}

impl SafeShutdownPlan {
    pub fn new(
        draining_replica_id: ReplicaId,
        replacement_replica_id: ReplicaId,
    ) -> Result<Self, ReplicationError> {
        if draining_replica_id == replacement_replica_id {
            return Err(ReplicationError::InvalidReplacementReplica {
                replica_id: draining_replica_id,
            });
        }
        Ok(Self {
            draining_replica_id,
            replacement_replica_id,
        })
    }
}

impl ActiveReplicatorState {
    pub fn fenced(
        replica_id: ReplicaId,
        leader_epoch: LeaderEpoch,
        watermark: SyncWatermark,
        durability_policy: DurabilityPolicy,
    ) -> Result<Self, ReplicationError> {
        if leader_epoch <= watermark.leader_epoch {
            return Err(ReplicationError::StaleFence {
                current_epoch: watermark.leader_epoch,
                requested_epoch: leader_epoch,
            });
        }
        Ok(Self {
            replica_id,
            leader_epoch,
            watermark: SyncWatermark {
                leader_epoch,
                ..watermark
            },
            durability_policy,
            readiness: ReplicaReadiness::ActiveWriter,
            in_flight_commits: 0,
        })
    }

    pub fn begin_commit(&mut self) -> Result<(), ReplicationError> {
        if self.readiness != ReplicaReadiness::ActiveWriter {
            return Err(ReplicationError::NotActiveWriter);
        }
        self.in_flight_commits = self.in_flight_commits.saturating_add(1);
        Ok(())
    }

    pub fn finish_commit(&mut self) {
        self.in_flight_commits = self.in_flight_commits.saturating_sub(1);
    }

    pub fn commit_record(
        &mut self,
        payload: Vec<u8>,
        record_hash: LogHash,
        proof: CommitProof,
    ) -> Result<CommittedRecord, ReplicationError> {
        if self.readiness != ReplicaReadiness::ActiveWriter {
            return Err(ReplicationError::NotActiveWriter);
        }
        let effective_acks = proof.effective_ack_count();
        if !self.durability_policy.is_satisfied_by(effective_acks) {
            return Err(ReplicationError::InsufficientDurability {
                index: self.watermark.committed_index.saturating_add(1),
                required_acks: match self.durability_policy {
                    DurabilityPolicy::LocalOnly => 0,
                    DurabilityPolicy::RequirePassiveAcks { min_acks } => min_acks,
                },
                actual_acks: effective_acks,
            });
        }
        let record = CommittedRecord {
            leader_epoch: self.leader_epoch,
            index: self.watermark.committed_index.saturating_add(1),
            previous_hash: self.watermark.last_applied_hash,
            record_hash,
            writer_replica_id: self.replica_id,
            payload,
            proof,
        };
        self.watermark = SyncWatermark {
            leader_epoch: self.leader_epoch,
            committed_index: record.index,
            applied_index: record.index,
            last_applied_hash: record.record_hash,
        };
        Ok(record)
    }

    pub fn start_draining(&mut self) -> Result<(), ReplicationError> {
        match self.readiness {
            ReplicaReadiness::ActiveWriter | ReplicaReadiness::Draining => {
                self.readiness = ReplicaReadiness::Draining;
                Ok(())
            }
            ReplicaReadiness::CleanShutdown => Ok(()),
            _ => Err(ReplicationError::CannotDrainReplica {
                readiness: self.readiness,
            }),
        }
    }

    pub fn start_safe_shutdown<T>(
        &mut self,
        traffic: &mut T,
        plan: SafeShutdownPlan,
    ) -> Result<(), ReplicationError>
    where
        T: TrafficDrain,
        T::Error: std::fmt::Display,
    {
        if plan.draining_replica_id != self.replica_id {
            return Err(ReplicationError::WrongShutdownReplica {
                expected: self.replica_id,
                actual: plan.draining_replica_id,
            });
        }
        traffic
            .block_new_requests(self.replica_id)
            .map_err(|err| ReplicationError::TrafficDrain(err.to_string()))?;
        self.readiness = ReplicaReadiness::TrafficDraining;
        traffic
            .route_to_replacement(self.replica_id, plan.replacement_replica_id)
            .map_err(|err| ReplicationError::TrafficDrain(err.to_string()))?;
        let routed_to = traffic
            .verify_routed_away(self.replica_id)
            .map_err(|err| ReplicationError::TrafficDrain(err.to_string()))?;
        if routed_to != Some(plan.replacement_replica_id) {
            return Err(ReplicationError::TrafficStillTargetsDrainingReplica {
                replica_id: self.replica_id,
            });
        }
        self.readiness = ReplicaReadiness::Draining;
        Ok(())
    }

    pub fn can_shutdown_cleanly(&self) -> bool {
        matches!(
            self.readiness,
            ReplicaReadiness::Draining | ReplicaReadiness::CleanShutdown
        ) && self.in_flight_commits == 0
            && self.watermark.is_fresh()
    }

    pub fn complete_clean_shutdown(&mut self) -> Result<SyncWatermark, ReplicationError> {
        if !self.can_shutdown_cleanly() {
            return Err(ReplicationError::ShutdownNotClean {
                in_flight_commits: self.in_flight_commits,
                watermark: self.watermark,
            });
        }
        self.readiness = ReplicaReadiness::CleanShutdown;
        Ok(self.watermark)
    }
}

pub trait ReplicationTransport {
    type Error;

    fn fetch_after(
        &self,
        watermark: SyncWatermark,
        max_records: u16,
    ) -> Result<CommittedBatch, Self::Error>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryCommittedLog {
    records: BTreeMap<LogIndex, CommittedRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InMemoryTrafficDrain {
    serving_replica_id: ReplicaId,
    states: BTreeMap<ReplicaId, TrafficDrainState>,
}

impl InMemoryTrafficDrain {
    pub fn new(serving_replica_id: ReplicaId) -> Self {
        let mut states = BTreeMap::new();
        states.insert(serving_replica_id, TrafficDrainState::Serving);
        Self {
            serving_replica_id,
            states,
        }
    }

    pub fn serving_replica_id(&self) -> ReplicaId {
        self.serving_replica_id
    }
}

impl TrafficDrain for InMemoryTrafficDrain {
    type Error = ReplicationError;

    fn block_new_requests(&mut self, replica_id: ReplicaId) -> Result<(), Self::Error> {
        self.states
            .insert(replica_id, TrafficDrainState::NewRequestsBlocked);
        Ok(())
    }

    fn route_to_replacement(
        &mut self,
        draining_replica_id: ReplicaId,
        replacement_replica_id: ReplicaId,
    ) -> Result<(), Self::Error> {
        if draining_replica_id == replacement_replica_id {
            return Err(ReplicationError::InvalidReplacementReplica {
                replica_id: draining_replica_id,
            });
        }
        self.serving_replica_id = replacement_replica_id;
        self.states.insert(
            draining_replica_id,
            TrafficDrainState::RoutedAway {
                replacement_replica_id,
            },
        );
        self.states
            .insert(replacement_replica_id, TrafficDrainState::Serving);
        Ok(())
    }

    fn verify_routed_away(
        &self,
        draining_replica_id: ReplicaId,
    ) -> Result<Option<ReplicaId>, Self::Error> {
        Ok(match self.states.get(&draining_replica_id) {
            Some(TrafficDrainState::RoutedAway {
                replacement_replica_id,
            }) => Some(*replacement_replica_id),
            _ => None,
        })
    }
}

impl InMemoryCommittedLog {
    pub fn append(&mut self, record: CommittedRecord) -> Result<(), ReplicationError> {
        if let Some(existing) = self.records.get(&record.index) {
            if existing == &record {
                return Ok(());
            }
            return Err(ReplicationError::ConflictingRecord {
                index: record.index,
            });
        }
        self.records.insert(record.index, record);
        Ok(())
    }

    pub fn fetch_after(&self, watermark: SyncWatermark, max_records: u16) -> CommittedBatch {
        if max_records == 0 {
            return CommittedBatch::empty();
        }
        let start = watermark.committed_index.saturating_add(1);
        let records = self
            .records
            .range(start..)
            .take(max_records as usize)
            .map(|(_, record)| record.clone())
            .collect();
        CommittedBatch { records }
    }
}

impl ReplicationTransport for InMemoryCommittedLog {
    type Error = ReplicationError;

    fn fetch_after(
        &self,
        watermark: SyncWatermark,
        max_records: u16,
    ) -> Result<CommittedBatch, Self::Error> {
        Ok(self.fetch_after(watermark, max_records))
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ReplicationError {
    #[error("replication batch is empty")]
    EmptyBatch,
    #[error("local applied_index {applied_index} lags committed_index {committed_index}")]
    StaleLocalWatermark {
        committed_index: LogIndex,
        applied_index: LogIndex,
    },
    #[error("replication expected index {expected}, got {actual}")]
    IndexGap {
        expected: LogIndex,
        actual: LogIndex,
    },
    #[error("replication previous hash mismatch at index {index}")]
    PreviousHashMismatch { index: LogIndex },
    #[error("leader epoch regressed from {previous} to {actual}")]
    LeaderEpochRegression {
        previous: LeaderEpoch,
        actual: LeaderEpoch,
    },
    #[error("record {index} has {actual_acks} durable acks, requires {required_acks}")]
    InsufficientDurability {
        index: LogIndex,
        required_acks: u16,
        actual_acks: u16,
    },
    #[error("requested leader epoch {requested_epoch} is not newer than current {current_epoch}")]
    StaleFence {
        current_epoch: LeaderEpoch,
        requested_epoch: LeaderEpoch,
    },
    #[error("replica is not the active writer")]
    NotActiveWriter,
    #[error("replica in readiness state {readiness:?} cannot start draining")]
    CannotDrainReplica { readiness: ReplicaReadiness },
    #[error("invalid replacement replica {replica_id}")]
    InvalidReplacementReplica { replica_id: ReplicaId },
    #[error("safe shutdown requested replica {actual}, expected {expected}")]
    WrongShutdownReplica {
        expected: ReplicaId,
        actual: ReplicaId,
    },
    #[error("traffic drain failed: {0}")]
    TrafficDrain(String),
    #[error("traffic still targets draining replica {replica_id}")]
    TrafficStillTargetsDrainingReplica { replica_id: ReplicaId },
    #[error(
        "replica cannot shutdown cleanly with {in_flight_commits} in-flight commits and watermark {watermark:?}"
    )]
    ShutdownNotClean {
        in_flight_commits: u32,
        watermark: SyncWatermark,
    },
    #[error("committed log conflict at index {index}")]
    ConflictingRecord { index: LogIndex },
    #[error("domain validation failed: {0}")]
    DomainValidation(String),
    #[error("domain apply failed: {0}")]
    DomainApply(String),
    #[error("applied watermark mismatch: expected {expected:?}, got {actual:?}")]
    AppliedWatermarkMismatch {
        expected: SyncWatermark,
        actual: SyncWatermark,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestDomain {
        watermark: SyncWatermark,
        applied_payloads: Vec<Vec<u8>>,
    }

    impl ReplicatedLmdbDomain for TestDomain {
        type Error = String;

        fn validate_record(&self, record: &CommittedRecord) -> Result<(), Self::Error> {
            if record.payload.is_empty() {
                return Err("empty payload".to_string());
            }
            Ok(())
        }

        fn validate_payload_chain(
            &self,
            _current: SyncWatermark,
            records: &[CommittedRecord],
        ) -> Result<(), Self::Error> {
            for record in records {
                self.validate_record(record)?;
            }
            Ok(())
        }

        fn classify_divergence(
            &self,
            local: SyncWatermark,
            remote: SyncWatermark,
            local_suffix: &[CommittedRecord],
            remote_suffix: &[CommittedRecord],
        ) -> Result<MergeDecision, Self::Error> {
            if local == remote {
                return Ok(MergeDecision::AlreadyCurrent);
            }
            if local.committed_index < remote.committed_index
                && local.last_applied_hash
                    == remote_suffix
                        .first()
                        .map(|record| record.previous_hash)
                        .unwrap_or(remote.last_applied_hash)
            {
                return Ok(MergeDecision::FastForward {
                    records_to_apply: remote_suffix.len(),
                });
            }
            if !local_suffix.is_empty()
                && local_suffix
                    .iter()
                    .all(|record| record.proof.durable_ack_count == 0)
            {
                return Ok(MergeDecision::QuarantineUncommittedSuffixThenFastForward {
                    quarantine_from_index: local_suffix[0].index,
                    records_to_apply: remote_suffix.len(),
                });
            }
            Ok(MergeDecision::RejectDivergedCommittedLineage {
                local_index: local.committed_index,
                remote_index: remote.committed_index,
            })
        }

        fn apply_committed_batch(
            &mut self,
            records: &[CommittedRecord],
        ) -> Result<SyncWatermark, Self::Error> {
            for record in records {
                self.applied_payloads.push(record.payload.clone());
                self.watermark = SyncWatermark {
                    leader_epoch: record.leader_epoch,
                    committed_index: record.index,
                    applied_index: record.index,
                    last_applied_hash: record.record_hash,
                };
            }
            Ok(self.watermark)
        }
    }

    fn hash(byte: u8) -> LogHash {
        [byte; 32]
    }

    fn record(index: LogIndex, previous_hash: LogHash, record_hash: LogHash) -> CommittedRecord {
        CommittedRecord {
            leader_epoch: 1,
            index,
            previous_hash,
            record_hash,
            writer_replica_id: 1,
            payload: vec![index as u8],
            proof: CommitProof::from_ack_bitmap(0b10),
        }
    }

    fn record_with_proof(
        index: LogIndex,
        previous_hash: LogHash,
        record_hash: LogHash,
        proof: CommitProof,
    ) -> CommittedRecord {
        CommittedRecord {
            proof,
            ..record(index, previous_hash, record_hash)
        }
    }

    #[test]
    fn validates_contiguous_committed_batch() {
        let batch = CommittedBatch {
            records: vec![record(1, [0; 32], hash(1)), record(2, hash(1), hash(2))],
        };
        let plan = validate_committed_batch(
            SyncWatermark::default(),
            &batch,
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("valid batch");
        assert_eq!(plan.first_index, 1);
        assert_eq!(plan.last_index, 2);
        assert_eq!(plan.target_watermark.last_applied_hash, hash(2));
    }

    #[test]
    fn rejects_gap_before_apply() {
        let batch = CommittedBatch {
            records: vec![record(2, [0; 32], hash(2))],
        };
        assert_eq!(
            validate_committed_batch(
                SyncWatermark::default(),
                &batch,
                DurabilityPolicy::LocalOnly,
            ),
            Err(ReplicationError::IndexGap {
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn rejects_previous_hash_mismatch() {
        let batch = CommittedBatch {
            records: vec![record(1, hash(9), hash(1))],
        };
        assert_eq!(
            validate_committed_batch(
                SyncWatermark::default(),
                &batch,
                DurabilityPolicy::LocalOnly,
            ),
            Err(ReplicationError::PreviousHashMismatch { index: 1 })
        );
    }

    #[test]
    fn rejects_insufficient_durability() {
        let mut first = record(1, [0; 32], hash(1));
        first.proof = CommitProof::local_only();
        let batch = CommittedBatch {
            records: vec![first],
        };
        assert_eq!(
            validate_committed_batch(
                SyncWatermark::default(),
                &batch,
                DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
            ),
            Err(ReplicationError::InsufficientDurability {
                index: 1,
                required_acks: 1,
                actual_acks: 0
            })
        );
    }

    #[test]
    fn active_writer_requires_new_fence_epoch() {
        let watermark = SyncWatermark {
            leader_epoch: 7,
            ..SyncWatermark::default()
        };
        assert_eq!(
            ActiveReplicatorState::fenced(1, 7, watermark, DurabilityPolicy::LocalOnly),
            Err(ReplicationError::StaleFence {
                current_epoch: 7,
                requested_epoch: 7
            })
        );
    }

    #[test]
    fn active_writer_refuses_to_ack_without_required_passive_acks() {
        let mut active = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence");
        assert_eq!(
            active.commit_record(vec![1], hash(1), CommitProof::local_only()),
            Err(ReplicationError::InsufficientDurability {
                index: 1,
                required_acks: 1,
                actual_acks: 0
            })
        );
    }

    #[test]
    fn two_backend_active_commits_only_after_passive_durable_ack() {
        let mut active = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence active");
        let mut passive_domain = TestDomain::default();
        let mut passive = PassiveReplicaState::new(2, SyncWatermark::default());
        let candidate = record(1, [0; 32], hash(1));

        passive
            .apply_batch(
                &mut passive_domain,
                &CommittedBatch {
                    records: vec![candidate.clone()],
                },
                DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
            )
            .expect("passive durable apply");
        let committed = active
            .commit_record(
                candidate.payload,
                candidate.record_hash,
                CommitProof::from_ack_bitmap(0b10),
            )
            .expect("ack after passive durable apply");

        assert_eq!(committed.index, 1);
        assert_eq!(active.watermark.committed_index, 1);
        assert_eq!(passive.watermark.committed_index, 1);
    }

    #[test]
    fn two_backend_active_fails_closed_when_passive_down_before_ack() {
        let mut active = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence active");

        assert_eq!(
            active.commit_record(vec![1], hash(1), CommitProof::local_only()),
            Err(ReplicationError::InsufficientDurability {
                index: 1,
                required_acks: 1,
                actual_acks: 0
            })
        );
        assert_eq!(active.watermark.committed_index, 0);
    }

    #[test]
    fn active_restart_after_local_append_before_passive_ack_does_not_advance_watermark() {
        let active_before_crash = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence active");
        let recovered = ActiveReplicatorState::fenced(
            1,
            2,
            active_before_crash.watermark,
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("restart with newer fence");

        assert_eq!(recovered.watermark.committed_index, 0);
        assert_eq!(recovered.watermark.last_applied_hash, [0; 32]);
    }

    #[test]
    fn draining_active_writer_refuses_new_commits_until_clean() {
        let mut active = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence");
        active.begin_commit().expect("begin commit");
        active.start_draining().expect("draining");
        assert_eq!(
            active.commit_record(vec![1], hash(1), CommitProof::from_ack_bitmap(0b10)),
            Err(ReplicationError::NotActiveWriter)
        );
        assert!(!active.can_shutdown_cleanly());
        active.finish_commit();
        assert!(active.can_shutdown_cleanly());
        assert_eq!(
            active.complete_clean_shutdown().expect("clean shutdown"),
            SyncWatermark {
                leader_epoch: 1,
                ..SyncWatermark::default()
            }
        );
        assert_eq!(active.readiness, ReplicaReadiness::CleanShutdown);
    }

    #[test]
    fn safe_shutdown_routes_traffic_to_replacement_before_draining() {
        let mut active = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence");
        let mut traffic = InMemoryTrafficDrain::new(1);
        active
            .start_safe_shutdown(
                &mut traffic,
                SafeShutdownPlan::new(1, 2).expect("shutdown plan"),
            )
            .expect("start safe shutdown");
        assert_eq!(traffic.serving_replica_id(), 2);
        assert_eq!(
            traffic.verify_routed_away(1).expect("verify route"),
            Some(2)
        );
        assert_eq!(active.readiness, ReplicaReadiness::Draining);
        assert!(active.can_shutdown_cleanly());
    }

    #[test]
    fn safe_shutdown_rejects_self_replacement() {
        assert_eq!(
            SafeShutdownPlan::new(1, 1),
            Err(ReplicationError::InvalidReplacementReplica { replica_id: 1 })
        );
    }

    #[derive(Default)]
    struct BrokenTrafficDrain;

    impl TrafficDrain for BrokenTrafficDrain {
        type Error = ReplicationError;

        fn block_new_requests(&mut self, _replica_id: ReplicaId) -> Result<(), Self::Error> {
            Ok(())
        }

        fn route_to_replacement(
            &mut self,
            _draining_replica_id: ReplicaId,
            _replacement_replica_id: ReplicaId,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn verify_routed_away(
            &self,
            _draining_replica_id: ReplicaId,
        ) -> Result<Option<ReplicaId>, Self::Error> {
            Ok(None)
        }
    }

    #[test]
    fn safe_shutdown_refuses_stop_when_gateway_still_targets_draining_backend() {
        let mut active = ActiveReplicatorState::fenced(
            1,
            1,
            SyncWatermark::default(),
            DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
        )
        .expect("fence");
        let mut traffic = BrokenTrafficDrain;

        assert_eq!(
            active.start_safe_shutdown(
                &mut traffic,
                SafeShutdownPlan::new(1, 2).expect("shutdown plan"),
            ),
            Err(ReplicationError::TrafficStillTargetsDrainingReplica { replica_id: 1 })
        );
        assert_eq!(active.readiness, ReplicaReadiness::TrafficDraining);
        assert!(!active.can_shutdown_cleanly());
    }

    #[test]
    fn passive_applies_batch_and_updates_readiness() {
        let batch = CommittedBatch {
            records: vec![record(1, [0; 32], hash(1))],
        };
        let mut domain = TestDomain::default();
        let mut passive = PassiveReplicaState::new(2, SyncWatermark::default());
        passive
            .apply_batch(
                &mut domain,
                &batch,
                DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
            )
            .expect("apply");
        assert_eq!(passive.readiness, ReplicaReadiness::FreshReadReplica);
        assert_eq!(passive.watermark.committed_index, 1);
        assert_eq!(domain.applied_payloads, vec![vec![1]]);
    }

    #[test]
    fn in_memory_log_fetches_after_watermark() {
        let mut log = InMemoryCommittedLog::default();
        log.append(record(1, [0; 32], hash(1))).expect("append 1");
        log.append(record(2, hash(1), hash(2))).expect("append 2");
        let batch = log.fetch_after(
            SyncWatermark {
                leader_epoch: 1,
                committed_index: 1,
                applied_index: 1,
                last_applied_hash: hash(1),
            },
            16,
        );
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].index, 2);
    }

    #[test]
    fn in_memory_log_rejects_conflicting_record_at_same_index() {
        let mut log = InMemoryCommittedLog::default();
        log.append(record(1, [0; 32], hash(1))).expect("append 1");

        assert_eq!(
            log.append(record(1, [0; 32], hash(9))),
            Err(ReplicationError::ConflictingRecord { index: 1 })
        );
    }

    #[test]
    fn dropped_sync_round_then_restart_fast_forwards_passive_from_lmdb_watermark() {
        let mut log = InMemoryCommittedLog::default();
        log.append(record(1, [0; 32], hash(1))).expect("append 1");
        log.append(record(2, hash(1), hash(2))).expect("append 2");
        log.append(record(3, hash(2), hash(3))).expect("append 3");

        let mut first_process_domain = TestDomain::default();
        let mut first_process_passive = PassiveReplicaState::new(2, SyncWatermark::default());
        first_process_passive
            .apply_batch(
                &mut first_process_domain,
                &log.fetch_after(first_process_passive.watermark, 1),
                DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
            )
            .expect("apply first record");

        let persisted_watermark = first_process_passive.watermark;
        let mut restarted_domain = TestDomain {
            watermark: persisted_watermark,
            applied_payloads: vec![vec![1]],
        };
        let mut restarted_passive = PassiveReplicaState::new(2, persisted_watermark);
        restarted_passive
            .apply_batch(
                &mut restarted_domain,
                &log.fetch_after(restarted_passive.watermark, 16),
                DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
            )
            .expect("restart fast-forward");

        assert_eq!(restarted_passive.watermark.committed_index, 3);
        assert_eq!(
            restarted_domain.applied_payloads,
            vec![vec![1], vec![2], vec![3]]
        );
    }

    #[test]
    fn deterministic_schedule_drops_reorders_and_recovers_without_lineage_divergence() {
        let mut log = InMemoryCommittedLog::default();
        log.append(record(1, [0; 32], hash(1))).expect("append 1");
        log.append(record(2, hash(1), hash(2))).expect("append 2");
        log.append(record(3, hash(2), hash(3))).expect("append 3");
        let mut domain = TestDomain::default();
        let mut passive = PassiveReplicaState::new(2, SyncWatermark::default());

        let dropped = log.fetch_after(passive.watermark, 1);
        assert_eq!(dropped.records[0].index, 1);

        let reordered = CommittedBatch {
            records: vec![record(2, hash(1), hash(2))],
        };
        assert_eq!(
            passive.apply_batch(&mut domain, &reordered, DurabilityPolicy::LocalOnly),
            Err(ReplicationError::IndexGap {
                expected: 1,
                actual: 2
            })
        );
        assert_eq!(passive.watermark.committed_index, 0);

        passive
            .apply_batch(
                &mut domain,
                &log.fetch_after(passive.watermark, 16),
                DurabilityPolicy::RequirePassiveAcks { min_acks: 1 },
            )
            .expect("ordered recovery");
        assert_eq!(passive.watermark.committed_index, 3);
        assert_eq!(passive.watermark.last_applied_hash, hash(3));
    }

    #[test]
    fn classifies_returning_old_leader_suffix_for_quarantine() {
        let domain = TestDomain::default();
        let local = SyncWatermark {
            leader_epoch: 1,
            committed_index: 3,
            applied_index: 3,
            last_applied_hash: hash(3),
        };
        let remote = SyncWatermark {
            leader_epoch: 2,
            committed_index: 2,
            applied_index: 2,
            last_applied_hash: hash(2),
        };
        let local_suffix = [record_with_proof(
            3,
            hash(2),
            hash(3),
            CommitProof::local_only(),
        )];
        let decision = domain
            .classify_divergence(local, remote, &local_suffix, &[])
            .expect("classify");
        assert_eq!(
            decision,
            MergeDecision::QuarantineUncommittedSuffixThenFastForward {
                quarantine_from_index: 3,
                records_to_apply: 0
            }
        );
    }

    #[test]
    fn classifies_clean_fast_forward_from_remote_suffix() {
        let domain = TestDomain::default();
        let local = SyncWatermark {
            leader_epoch: 1,
            committed_index: 1,
            applied_index: 1,
            last_applied_hash: hash(1),
        };
        let remote = SyncWatermark {
            leader_epoch: 2,
            committed_index: 3,
            applied_index: 3,
            last_applied_hash: hash(3),
        };
        let remote_suffix = [record(2, hash(1), hash(2)), record(3, hash(2), hash(3))];

        assert_eq!(
            domain
                .classify_divergence(local, remote, &[], &remote_suffix)
                .expect("classify"),
            MergeDecision::FastForward {
                records_to_apply: 2
            }
        );
    }

    #[test]
    fn classifies_same_index_different_hash_as_diverged_committed_lineage() {
        let domain = TestDomain::default();
        let local = SyncWatermark {
            leader_epoch: 2,
            committed_index: 2,
            applied_index: 2,
            last_applied_hash: hash(2),
        };
        let remote = SyncWatermark {
            leader_epoch: 2,
            committed_index: 2,
            applied_index: 2,
            last_applied_hash: hash(9),
        };

        assert_eq!(
            domain
                .classify_divergence(local, remote, &[], &[])
                .expect("classify"),
            MergeDecision::RejectDivergedCommittedLineage {
                local_index: 2,
                remote_index: 2
            }
        );
    }

    #[test]
    fn classifies_conflicting_unacked_suffix_as_quarantine_then_fast_forward() {
        let domain = TestDomain::default();
        let local = SyncWatermark {
            leader_epoch: 1,
            committed_index: 2,
            applied_index: 2,
            last_applied_hash: hash(8),
        };
        let remote = SyncWatermark {
            leader_epoch: 2,
            committed_index: 3,
            applied_index: 3,
            last_applied_hash: hash(3),
        };
        let local_suffix = [record_with_proof(
            2,
            hash(1),
            hash(8),
            CommitProof::local_only(),
        )];
        let remote_suffix = [record(2, hash(1), hash(2)), record(3, hash(2), hash(3))];

        assert_eq!(
            domain
                .classify_divergence(local, remote, &local_suffix, &remote_suffix)
                .expect("classify"),
            MergeDecision::QuarantineUncommittedSuffixThenFastForward {
                quarantine_from_index: 2,
                records_to_apply: 2
            }
        );
    }
}
