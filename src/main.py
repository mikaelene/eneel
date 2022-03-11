import argparse
from pkg_resources import get_distribution

from src.models import Job
from src.utils import read_yml
from src.parser import job_to_extract_load_tasks
from src.runner import runner_extract_load_snowflake_task


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "project", help="The path to your project yml file. I.e my_project.yml"
    )
    args = parser.parse_args()

    if not args.project:
        print(
            "You need to supply the path to your project yml file. I.e my_project.yml"
        )

    else:
        project = args.project

        print('')
        print(f'Running eneel {get_distribution("eneel").version}')
        print('')

        try:
            job = Job(**read_yml(project))

            el_tasks = job_to_extract_load_tasks(job)

            runner_extract_load_snowflake_task(el_tasks, max_workers=job.parallel_loads)

        except KeyboardInterrupt:
            print("Interupted by user")


if __name__ == "__main__":
    main()
