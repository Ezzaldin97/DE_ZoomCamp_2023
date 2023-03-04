from prefect import flow
from source_to_gcs import el_gcs
from typing import List
from tqdm import tqdm

# this will work as parent workflow
@flow(name = "ParentEL",
      description = "This Function Manages the Workflow of EL Workflow to run for different parameters..",
      log_prints=True)
def el_parent_gcs_flow(types:List[str] = ["yellow"], months:List[int] = list(range(3, 13)), years:List[int]=[2019, 2020]):
    for car_type in types:
        for year in years:
            for month in months:
                el_gcs(car_type, year, month)

if __name__ == "__main__":
    el_parent_gcs_flow()