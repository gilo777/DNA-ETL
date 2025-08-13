import json

from Pipeline.DataModels.ValidPaths import ValidPaths


class Load:
    """
    Responsible for formatting processed DNA data and metadata into a structured
    output format and saving it as a JSON file with processing metadata included.

    Methods:
        create_output(meta_data : dict, dna_data : dict, paths : ValidPaths, start_time : datetime, end_time : datetime, participant_id : str) -> None:
            Creates and saves a structured output file containing processed genetic data and processing metadata.
    """
    def create_output(self, meta_data : dict, dna_data : dict, paths : ValidPaths, start_time : str, end_time : str, participant_id : str) -> None:
        """
        Combines processed DNA data and metadata with processing information (timing, paths)
        into a standardized JSON output format and persists it to the specified output path.

        :param meta_data: Processed metadata dictionary containing participant information
        :param dna_data: Transformed DNA sequence data
        :param paths: Object containing input/output file paths
        :param start_time: Timestamp when processing began
        :param end_time: Timestamp when processing completed
        :param participant_id: Unique identifier for the participant
        :return:
            dict: The complete output dictionary if save succeeds, None if save fails

        Side Effects:
            - Creates/overwrites file at paths.output_path
            - Prints success/error messages to console
        """
        # Create a dictionary containing processed data as required.
        output = {
            "metadata": {
                "start_at": str(start_time),
                "end_at": str(end_time),
                "context_path": str(paths.context_path),
                "results_path": str(paths.output_path),
            },
            "results": [
                {
                    "participant": {"_id": participant_id},
                    "txt": dna_data,
                    "json": meta_data,
                }
            ],
        }
        # Loads the dictionary content as Json to the output path.
        try:
            with open(paths.output_path, 'w+') as f:
                json.dump(output, f, indent = 4)
            print(f"Output saved successfully to: {paths.output_path}")
            return output
        except Exception as e:
            print(f"Error saving output: {e}")
            return None
