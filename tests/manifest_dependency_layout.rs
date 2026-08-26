// Guard for the manifest table layout that broke locked Linux release builds.
//
// Upstream #17 added a `[target.'cfg(target_os = "macos")'.dependencies]`
// section directly above the optional dependency list without re-opening a
// plain table afterwards. TOML keeps every following entry inside that target
// table, so `blake3`, `tokio-postgres`, `tokio`, and `proptest` silently became
// macOS-only dependencies while `migrations = ["dep:blake3"]`, `postgres-sync`,
// and `migrations-test` kept compiling their modules on every platform.
// The first Linux consumer enabling `migrations` then failed E0433
// (unresolved crate `blake3`) — invisible on macOS, where everything always
// resolved, and unexercised until an immutable-pinned Linux release build ran.
//
// This file intentionally reads Cargo.toml as text: the failure mode is table
// placement itself, not resolution behavior, and placement must hold on every
// host so it cannot regress the same way again.

const MANIFEST: &str = include_str!("../Cargo.toml");

fn table_regions() -> (Vec<&'static str>, Vec<&'static str>) {
    let mut before_target = Vec::new();
    let mut inside_target = Vec::new();
    let mut seen_target = false;
    for line in MANIFEST.lines() {
        if line.starts_with("[target.") {
            seen_target = true;
        }
        if seen_target {
            inside_target.push(line);
        } else {
            before_target.push(line);
        }
    }
    (before_target, inside_target)
}

#[test]
fn optional_dependencies_stay_outside_platform_tables() {
    let (before_target, _) = table_regions();
    let head = before_target.join("\n");
    for required in [
        "blake3",
        "tokio-postgres",
        "tokio",
        "proptest",
    ] {
        assert!(
            head.contains(&format!("{required} = {{")),
            "`{required}` must be declared before the first [target.*] table. \
             A placement below it hides the dependency from Linux builds and \
             breaks `migrations`, `postgres-sync`, or `migrations-test` there."
        );
    }
}

#[test]
fn macos_posix_sem_heed_override_stays_target_scoped() {
    // The whole point of the platform table must survive the fix.
    let (_, inside_target) = table_regions();
    let tail = inside_target.join("\n");
    assert!(
        tail.contains("features = [\"posix-sem\"]"),
        "the macOS-only posix-sem heed override disappeared"
    );
    assert!(
        !tail.contains("blake3"),
        "blake3 must not live inside a platform table"
    );
}
