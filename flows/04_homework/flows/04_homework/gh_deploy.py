from prefect.filesystems import GitHub
from parameterized_flow_github import etl_parent_flow
from prefect.deployments import Deployment

github_block = GitHub.load("zoomcamp")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow, name="github-to-gcs-flow", infrastructure=github_block
)

if __name__ == "__main__":
    github_dep.apply()
