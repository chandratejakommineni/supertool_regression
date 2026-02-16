"""Clean cmc_ni_co_data_prep.ipynb: remove empty cells, clear outputs, light section renumbering."""
import json
from pathlib import Path

NB_PATH = Path(__file__).resolve().parent / "cmc_ni_co_data_prep.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

cells = nb["cells"]

def strip_outputs(c):
    out = dict(c)
    out["source"] = c["source"]
    if c["cell_type"] == "code":
        out["outputs"] = []
        out["execution_count"] = None
    return out

# Indices to keep (drop 18 and 25 - empty code cells)
keep_ix = [i for i in range(len(cells)) if i not in (18, 25)]
new_cells = [strip_outputs(cells[i]) for i in keep_ix]

# Optional: consistent subsection labels (markdown only)
for c in new_cells:
    if c["cell_type"] != "markdown" or not c["source"]:
        continue
    first = c["source"][0] if c["source"] else ""
    if first == "## Missing values: master list and incoming materials\n":
        c["source"][0] = "## 2.1 Missing values: master list and incoming materials\n"
    elif first == "### Inspect: rows with quantity == 0 and rows with no composition match\n":
        c["source"][0] = "### 3.1 Inspect: rows with quantity == 0 and no composition match\n"
    elif first == "## Analysis: sample and transaction frequency (historical)\n":
        c["source"][0] = "## 3.2 Analysis: sample and transaction frequency (historical)\n"
    elif first == "### Incoming material: shift-wise count and weight\n":
        c["source"][0] = "### 3.2a Incoming material: shift-wise count and weight\n"
    elif first == "## Feature map (ML table)\n":
        c["source"][0] = "## 4.1 Feature map (ML table)\n"

nb["cells"] = new_cells
with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=2, ensure_ascii=False)

print("Done. Cells:", len(new_cells), "(removed 2 empty). Outputs cleared.")
