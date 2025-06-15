from pathlib import Path
import importlib

map_projections = {}

current_dir = Path(__file__).parent
for file in current_dir.glob('[!_]*.py'):
    module_name = file.stem
    if not module_name.startswith('_'):
        module = importlib.import_module(
            f'.{module_name}',
            package=__package__
        )
        if hasattr(module, module_name.replace(" ", "")):
            map_projections[module_name] = getattr(
                module,
                module_name.replace(" ", "")
            )
