from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_only_guarded_snowflake_loader_is_supported():
    legacy_loader = (
        PROJECT_ROOT
        / "scripts"
        / "load_to_snowflake.py"
    )

    guarded_loader = (
        PROJECT_ROOT
        / "scripts"
        / "run_snowflake_sql.py"
    )

    assert not legacy_loader.exists()
    assert guarded_loader.is_file()
