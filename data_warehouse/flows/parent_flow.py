from prefect import flow
from source_to_gcs import el_gcs
from typing import List
from tqdm import tqdm

# this will work as parent workflow
@flow(name = "ParentEL",
      description = "This Function Manages the Workflow of EL Workflow to run for different parameters..",
      log_prints=True)
def el_parent_gcs_flow(months:List[int], years:List[int]=[2019]):
    for year in tqdm(years):
        for month in tqdm(months):
            el_gcs(year, month)

if __name__ == "__main__":
    months = list(range(1, 13))
    el_parent_gcs_flow(months)