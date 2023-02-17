from prefect.filesystems import GitHub
from parameterized_flow_github import etl_parent_flow
from prefect.deployments import Deployment

github_block = GitHub(
    repository="https://github.com/stanchiu224/prefect-zoomcamp.git", reference="main"
)
github_block.get_directory("flows/04_homework")
github_block.save("de-zoomcamp")

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-to-gcs-flow",
    parameters={"color": "green", "months": [11], "years": [2020]},
)

if __name__ == "__main__":
    github_dep.apply()
