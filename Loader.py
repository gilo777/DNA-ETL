import json


class Loader:


    def load(self, meta_data, dna_data, paths, start_time, end_time, participant_id) -> None:
        output = {
            "metadata": {
                "start_at": start_time,
                "end_at": end_time,
                "context_path": str(paths.dna_path),
                "results_path": str(paths.metadata_path),
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
            with open(paths.output_path, 'w+', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            print(f"Output saved successfully to: {paths.output_path}")
            return output
        except Exception as e:
            print(f"Error saving output: {e}")
            return None
