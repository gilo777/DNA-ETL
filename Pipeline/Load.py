import json

class Load:
    """

    """
    def create_output(self, meta_data, dna_data, paths, start_time, end_time, participant_id) -> None:
        """

        :param meta_data:
        :param dna_data:
        :param paths:
        :param start_time:
        :param end_time:
        :param participant_id:
        :return:
        """
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
        try:
            with open(paths.output_path, 'w+') as f:
                json.dump(output, f, indent = 4)
            print(f"Output saved successfully to: {paths.output_path}")
            return output
        except Exception as e:
            print(f"Error saving output: {e}")
            return None
