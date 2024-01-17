# %% [markdown]
# ## Helpers
# %%
crop_from_task = {
    "Plant Beans": "Soybean",
    "Seed Cover Crop Mix": "Cover crop",
    "Plant Corn": "Corn",
    "Seed Rye": "Rye",
    "Seed Oats": "Oats",
}


def translate_crop_from_task(task):
    if not isinstance(task, str):
        return task
    return crop_from_task.get(task, task)


def mark_fuel_ops(product_name):
    infer = 0
    if "diesel" in product_name.lower() or product_name == "Aerial dry":
        infer = 1
    return infer
