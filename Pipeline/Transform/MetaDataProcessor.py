class MetaDataProcessor:
    """

    """
    # Remove all related sensitive data in a metadata dict :
    def remove_private_keys(self, meta_data: dict) -> dict:
        """

        :param meta_data:
        :return:
        """
        cleaned_dict = {}

        for key, value in meta_data.items():
            if key.startswith("_"):
                continue

            if isinstance(value, dict):
                cleaned_dict[key] = self.remove_private_keys(value)

            else:
                cleaned_dict[key] = value

        return cleaned_dict