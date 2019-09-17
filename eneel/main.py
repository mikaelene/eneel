import eneel.load_runner as load_runner
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("connections")
    parser.add_argument("project")
    args = parser.parse_args()

    if not args.connections:
        print("You need to supply a path to your connections.yml")
    elif not args.project:
        print("You need to supply a path to your project.yml")
    else:
        print("Connections config: ", args.connections)
        print("Project config: ", args.project)
        try:
            load_runner.run_project(args.connections, args.project)
        except KeyboardInterrupt:
            print("Interupted by user")


if __name__ == '__main__':
    main()

