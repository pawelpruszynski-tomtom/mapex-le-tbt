"""New Kedro Project file for ensuring the package is executable
as `new-kedro-project` and `python -m new_kedro_project`
"""
import importlib  # pragma: no cover
from pathlib import Path  # pragma: no cover

from kedro.framework.cli.utils import (  # pragma: no cover
    KedroCliError,
    load_entry_points,
)
from kedro.framework.project import configure_project  # pragma: no cover


def _find_run_command(package_name):  # pragma: no cover
    try:
        project_cli = importlib.import_module(f"{package_name}.cli")
        # fail gracefully if cli.py does not exist
    except ModuleNotFoundError as exc:
        if f"{package_name}.cli" not in str(exc):
            raise
        plugins = load_entry_points("project")
        run = _find_run_command_in_plugins(plugins) if plugins else None
        if run:
            # use run command from installed plugin if it exists
            return run
        # use run command from `kedro.framework.cli.project`
        from kedro.framework.cli.project import run

        return run
    # fail badly if cli.py exists, but has no `cli` in it
    if not hasattr(project_cli, "cli"):
        raise KedroCliError(f"Cannot load commands from {package_name}.cli")
    return project_cli.run


def _find_run_command_in_plugins(plugins):  # pragma: no cover
    for group in plugins:
        if "run" in group.commands:
            return group.commands["run"]


def main(*args, **kwargs):  # pragma: no cover
    package_name = Path(__file__).parent.name
    configure_project(package_name)
    run = _find_run_command(package_name)
    run(*args, **kwargs)


if __name__ == "__main__":  # pragma: no cover
    main()
