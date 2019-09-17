import eneel.load_runner as load_runner
import argparse
import eneel.logger as logger
logger = logger.get_logger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("connections")
    parser.add_argument("project")
    args = parser.parse_args()

    if not args.connections:
        logger.error("You need to supply a path to your connections.yml")
    elif not args.project:
        logger.error("You need to supply a path to your project.yml")
    else:
        logger.info("Connections config: " + args.connections)
        logger.info("Project config: " + args.project)
        try:
            load_runner.run_project(args.connections, args.project)
        except KeyboardInterrupt:
            logger.warning("Interupted by user")


if __name__ == '__main__':
    main()

