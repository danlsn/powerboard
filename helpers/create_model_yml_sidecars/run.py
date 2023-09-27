import re
import subprocess
from pathlib import Path

from ruamel.yaml import YAML
from tqdm import tqdm


def get_models_from_dir(models_dir):
    models = []
    for path in Path(models_dir).glob("*.sql"):
        models.append(path.stem)
    return models


def generate_model_yaml(model_name, upstream_descriptions=True):
    args = f"""{{"model_names": ["{model_name}"], "upstream_descriptions": {upstream_descriptions}}}"""
    process = subprocess.Popen(
        ["dbt", "run-operation", "generate_model_yaml", "--args", args],
        cwd=Path("../../powerboard_dbt"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8")
    model_yaml_pat = re.compile(r".*(version: 2.*)\x1b\[0m", re.DOTALL)
    match = model_yaml_pat.match(stdout)
    if match:
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.explicit_start = True
        yaml.explicit_end = True
        yaml.preserve_quotes = True
        yaml.width = 1000
        model_yaml = yaml.load(match.group(1))
        return model_yaml


def get_existing_model_yaml(models_dir):
    model_yamls = {}
    for path in Path(models_dir).rglob("*.yml"):
        model_yamls[path.stem] = YAML().load(path)
    return model_yamls


def main(models_dir="../../pivot_redshift/models/staging/classic", overwrite=False):
    models = get_models_from_dir(models_dir)
    existing_yamls = get_existing_model_yaml(models_dir)
    pbar = tqdm(models, desc="Generating model YAMLs", unit="model")
    written = 0
    skipped = 0
    for model in pbar:
        pbar.set_description(f"Generating YAML for {model}")
        if overwrite or model not in existing_yamls:
            model_yaml = generate_model_yaml(model)
            if model_yaml:
                with open(f"{models_dir}/{model}.yml", "w") as f:
                    YAML().dump(model_yaml, f)
                written += 1
                pbar.set_postfix({"written": written, "skipped": skipped})
        else:
            pbar.set_description(f"Not overwriting {model}")
            skipped += 1
            pbar.set_postfix({"written": written, "skipped": skipped})

    ...


if __name__ == "__main__":
    dbt_package_path = Path("../../powerboard_dbt")
    models_dir = Path(
        input(
            f"Enter models directory relative to dbt project ({dbt_package_path.name}): "
        )
    )
    main(models_dir=dbt_package_path / models_dir, overwrite=False)
