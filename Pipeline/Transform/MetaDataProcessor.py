class MetaDataProcessor:
    """
    Processes and sanitizes metadata by removing sensitive or private information.

    Provides data cleaning functionality to remove private keys (identified by underscore prefix)
    from metadata dictionaries while preserving the overall structure and non-sensitive data.

    Methods:
        remove_private_keys(meta_data : dict) -> dict: Removes private keys from metadata dictionaries.
    """

    def remove_private_keys(self, meta_data: dict) -> dict:
        """
        Recursively removes all private keys from a metadata dictionary.

        Creates a sanitized copy of the input dictionary by filtering out keys that
        start with underscore ("_").

        :param meta_data: Original metadata dictionary potentially containing private keys
        :return:
            dict: New dictionary with all private keys removed, maintaining the original
                  structure for non-private data
        """
        cleaned_dict = {}
        for key, value in meta_data.items():
            # Skip private key.
            if key.startswith("_"):
                continue
            # Recursively clean nested dictionary
            if isinstance(value, dict):
                cleaned_dict[key] = self.remove_private_keys(value)
            # Copy not private key into the new dictionary
            else:
                cleaned_dict[key] = value

        return cleaned_dict
