# %%

import sys

sys.path.append("..")

from utils.data_loader import load_data

# %%

buildings = load_data(place="Spain", data_type="buildings")

# %%

buildings.head()
# %%