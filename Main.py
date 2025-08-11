from Orchestrator import Orchestrator


def main():
    # Create the orchestrator instance
    orchestrator = Orchestrator()

    # Ask the user to provide the path to their JSON configuration file
    config_path = input("Enter the path to your JSON configuration file: ")

    # Run the pipeline with the user-provided path
    result = orchestrator.orchestrate(config_path)

    # Print the result
    print(f"Pipeline completed: {result}")


if __name__ == "__main__":
    main()
