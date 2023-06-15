import itertools as it

from lazylines import LazyLines

dataset = [
    {"_task_hash": 1, "_annotator_id": 1, "answer": "accept"},
    {"_task_hash": 1, "_annotator_id": 2, "answer": "accept"},
    {"_task_hash": 1, "_annotator_id": 3, "answer": "reject"},
    {"_task_hash": 1, "_annotator_id": 4, "answer": "reject"},
    {"_task_hash": 2, "_annotator_id": 1, "answer": "accept"},
    {"_task_hash": 2, "_annotator_id": 2, "answer": "accept"},
    {"_task_hash": 2, "_annotator_id": 3, "answer": "accept"},
    {"_task_hash": 3, "_annotator_id": 2, "answer": "accept"},
]

def to_nested_pairs(lines, subset_key="subset"):
    for ex in lines:
        for c1, c2 in it.combinations(ex[subset_key], r=2):
            yield {**ex, "subset": [c1, c2]}

def pluck_values(arr, key):
    return [ex[key] for ex in arr]

stats = (LazyLines(dataset)
  .nest_by("_task_hash")
  .pipe(to_nested_pairs)
  .mutate(agree=lambda d: d["subset"][0]["answer"] == d["subset"][1]["answer"],
          annot_a=lambda d: d["subset"][0]["_annotator_id"],
          annot_b=lambda d: d["subset"][1]["_annotator_id"])
  .drop("subset", "_task_hash")
  .nest_by("annot_a", "annot_b")
  .mutate(agreements=lambda d: pluck_values(d["subset"], "agree"),
          n_overlap = lambda d: len(d["agreements"]),
          mean_agreement = lambda d: sum(d["agreements"])/d["n_overlap"])
  .drop("agree", "subset", "agreements")
  .show(6)
  .collect()
)
