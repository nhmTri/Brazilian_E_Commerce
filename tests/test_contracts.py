"""Checks that need no cluster: the code parses, the notebooks are valid, and
the SCD2 engine still exposes the interface its own docstring promises."""
import ast, json, pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
failures = []


def check(label, ok, detail=""):
    print(("  ok   " if ok else "  FAIL ") + label + (("  — " + detail) if detail and not ok else ""))
    if not ok:
        failures.append(label)


def main():
    print("python sources parse")
    for path in sorted(ROOT.joinpath("src").rglob("*.py")):
        rel = path.relative_to(ROOT)
        try:
            ast.parse(path.read_text(encoding="utf-8"))
            check(str(rel), True)
        except SyntaxError as exc:
            check(str(rel), False, str(exc))

    print("notebooks are valid json with cells")
    for path in sorted(ROOT.rglob("*.ipynb")):
        rel = path.relative_to(ROOT)
        try:
            nb = json.loads(path.read_text(encoding="utf-8"))
            check(str(rel), isinstance(nb.get("cells"), list) and len(nb["cells"]) > 0,
                  "no cells")
        except json.JSONDecodeError as exc:
            check(str(rel), False, str(exc))

    print("the SCD2 engine keeps its documented interface")
    engine = ROOT / "src" / "scd" / "scd2_engine.py"
    tree = ast.parse(engine.read_text(encoding="utf-8"))
    classes = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
    check("class SCD2Engine exists", "SCD2Engine" in classes)
    if "SCD2Engine" in classes:
        methods = {n.name: n for n in classes["SCD2Engine"].body if isinstance(n, ast.FunctionDef)}
        check("SCD2Engine.apply exists", "apply" in methods)
        if "apply" in methods:
            args = {a.arg for a in methods["apply"].args.args}
            for required in ("source_df", "target_table", "natural_key",
                             "tracked_cols", "surrogate_col"):
                check(f"apply(..., {required})", required in args,
                      "the README and the docstring both promise it")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
