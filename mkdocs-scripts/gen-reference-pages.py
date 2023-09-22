from pathlib import Path
import mkdocs_gen_files

# mkdocstrings is not able to parse these modules correctly!
skip = ["plpipes.database.driver.azure_sql",
        "plpipes.database.driver.plugin.azure_sql"]

# Ruta a tu código fuente (ajusta si no es "src")
src_root = Path("src")
# Nombre del paquete principal (ajusta a lo tuyo)

# Recorre todos los archivos .py dentro de src/
for former_path in sorted(src_root.rglob("*.py")):
    path = former_path.relative_to(src_root)
    if path.name == "__init__.py":
        path = path.parent # with_name("base")
    else:
        path = path.with_name(path.stem) # + "1") #with_suffix("")
    module = ".".join(path.parts)
    doc = path.with_suffix(".md")

    if module in skip:
        print(f"Skipping {module}")
        continue
    # print(f"Saving file {doc} for module {module}")

    with mkdocs_gen_files.open("reference/"+str(doc), "w") as f:
        f.write(f"""
# {module}
::: {module}
""")
    # Añade la entrada a SUMMARY.md (para literate-nav)
    with mkdocs_gen_files.open("reference/summary.md", "a") as nav:
        print(f"- [`{module}`]({doc})", file=nav)
