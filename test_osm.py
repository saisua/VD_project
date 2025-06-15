# import shutil
from utils.data_download.osm import load_data


# shutil.rmtree("data/osm_chunks")
# %%

buildings = load_data(place="Spain", data_type="buildings", force_download=True)

# %%

buildings.head()
# %%
