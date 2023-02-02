from prefect.deployments import Deployment
from gcs_flow_github import github_web_to_gcs
from prefect.filesystems import GitHub

github_block = GitHub. load("github-zoom")

github_dep = Deployment.build_from_flow(
    flow=github_web_to_gcs,
    name="github-flow",
    storage=github_block,
)


if __name__ == "__main__":
    github_dep.apply()