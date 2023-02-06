from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from flows.el_gcs_bq import el_parent_bq_flow


docker_container_block = DockerContainer.load("hm-to-bq")
docker_dep = Deployment.build_from_flow(
    flow = el_parent_bq_flow,
    name = "hm-to-bq",
    infrastructure = docker_container_block
)

if __name__ == "__main__":
    docker_dep.apply()