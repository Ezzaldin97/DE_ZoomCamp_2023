from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from flows.to_gcs import etl_parent_flow


docker_container_block = DockerContainer.load("hm-to-gcs")
docker_dep = Deployment.build_from_flow(
    flow = etl_parent_flow,
    name = "hm-to-gcs",
    infrastructure = docker_container_block
)

if __name__ == "__main__":
    docker_dep.apply()