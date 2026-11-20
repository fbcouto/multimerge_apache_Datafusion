#!/bin/bash
# DataFusion's own SQL test suite (sqllogictest) with the MultiMerge rule installed.
#
#   scripts/sqllogictest.sh setup [DIR]       clone DataFusion 55.3.0 into DIR (default:
#                                             ../datafusion-55.3.0) with its test data, make its
#                                             test runner depend on this crate, and build it
#   scripts/sqllogictest.sh run MODE [DIR]    run the whole suite; MODE is
#                                               base   without the rule (DataFusion as released)
#                                               final  Mode::FinalMerge
#                                               whole  Mode::WholeSort
#
# The only change to DataFusion is in the runner: where it builds the session of each test file
# (TestContext::try_new_for_test_file), MULTIMERGE_MODE=final|whole adds the rule with
# `multimerge_datafusion::optimizer_rules`. Without the variable the runner is unchanged.
# The output of each run goes to slt_MODE.log in the current directory.
set -o pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
case "$1" in
setup) DF="${2:-$HERE/../datafusion-55.3.0}" ;;
run) DF="${3:-$HERE/../datafusion-55.3.0}" ;;
esac

profile() {
    if grep -q '^\[profile.ci\]' "$DF/Cargo.toml"; then echo "--profile ci"; fi
}

case "$1" in
setup)
    if [ ! -d "$DF" ]; then
        git clone --depth 1 --branch 55.3.0 https://github.com/apache/datafusion.git "$DF" || exit 1
    fi
    # The test data (testing/, parquet-testing/) are git submodules: without them dozens of
    # files that read CSV, Parquet and Avro fail, with or without the rule.
    (cd "$DF" && git submodule update --init --depth 1) || exit 1
    python3 - "$DF" "$HERE" <<'EOF'
import io, sys
df, crate = sys.argv[1], sys.argv[2]

def replace(path, old, new):
    text = io.open(path, encoding="utf-8").read()
    if new in text:
        print("already applied:", path)
        return
    assert text.count(old) == 1, (path, text.count(old))
    io.open(path, "w", encoding="utf-8").write(text.replace(old, new))
    print("applied:", path)

# This crate depends on `datafusion` from crates.io; the patch points it to the local checkout.
root = df + "/Cargo.toml"
text = io.open(root, encoding="utf-8").read()
if "[patch.crates-io]" not in text:
    io.open(root, "w", encoding="utf-8").write(
        text + '\n[patch.crates-io]\ndatafusion = { path = "datafusion/core" }\n')
    print("applied:", root)

replace(df + "/datafusion/sqllogictest/Cargo.toml",
        "[dependencies]\n",
        '[dependencies]\nmultimerge-datafusion = { path = "%s" }\n' % crate)

replace(df + "/datafusion/sqllogictest/src/test_context.rs",
        """        let mut state_builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features();
""",
        """        let mut state_builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features();

        // multimerge-datafusion: MULTIMERGE_MODE=final|whole installs its sort rule.
        let mode = match std::env::var("MULTIMERGE_MODE").as_deref() {
            Ok("final") => Some(multimerge_datafusion::Mode::FinalMerge),
            Ok("whole") => Some(multimerge_datafusion::Mode::WholeSort),
            _ => None,
        };
        if let Some(mode) = mode {
            state_builder = state_builder
                .with_physical_optimizer_rules(multimerge_datafusion::optimizer_rules(mode));
        }
""")
EOF
    cd "$DF" && cargo build $(profile) -p datafusion-sqllogictest --test sqllogictests
    ;;
run)
    mode="${2:-base}"
    log="$PWD/slt_$mode.log"
    cd "$DF" || exit 1
    if [ "$mode" = base ]; then unset MULTIMERGE_MODE; else export MULTIMERGE_MODE="$mode"; fi
    cargo test $(profile) -p datafusion-sqllogictest --test sqllogictests > "$log" 2>&1
    echo "mode $mode: exit code $? (log: $log)"
    tail -3 "$log"
    ;;
*)
    echo "usage: $0 setup [DIR] | run base|final|whole [DIR]"
    exit 1
    ;;
esac
