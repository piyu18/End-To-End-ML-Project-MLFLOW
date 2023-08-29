import setuptools   

with open("README.md", "r", encoding='utf-8') as f:
    long_description = f.read()

REPO_NAME = 'End-To-End-ML-Project-MLFLOW'
USER_NAME = 'piyu18'

setuptools.setup(

    name = 'end_to_end_ml_project_mlflow',
    version = '0.0.1',
    author = 'Priya',
    author_email = 'priya1803singh@gmail.com',
    description = 'ML Pipeline using MLFLOW',
    long_description= long_description,
    long_description_content = 'text/markdown',
    url = f"https://github.com/{USER_NAME}/{REPO_NAME}",
    project_urls = {
        "BugTracker": f"https://github.com/{USER_NAME}/{REPO_NAME}/issues"
    },
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src")

)