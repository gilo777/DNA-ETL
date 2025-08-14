import unittest
from Pipeline.Transform.MetaDataProcessor import MetaDataProcessor


class TestMetaDataProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = MetaDataProcessor()

    def test_remove_private_keys_simple(self):
        metadata = {
            "name": "John Doe",
            "_private_id": "secret123",
            "age": 45,
            "_internal_note": "confidential"
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "name": "John Doe",
            "age": 45
        }

        self.assertEqual(result, expected)
        self.assertNotIn("_private_id", result)
        self.assertNotIn("_internal_note", result)

    def test_remove_private_keys_nested(self):
        metadata = {
            "participant": {
                "name": "Jane Doe",
                "_ssn": "123-45-6789",
                "contact": {
                    "email": "jane@example.com",
                    "_phone": "555-1234"
                }
            },
            "_system_id": "sys123"
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "participant": {
                "name": "Jane Doe",
                "contact": {
                    "email": "jane@example.com"
                }
            }
        }

        self.assertEqual(result, expected)
        self.assertNotIn("_system_id", result)
        self.assertNotIn("_ssn", result["participant"])
        self.assertNotIn("_phone", result["participant"]["contact"])

    def test_remove_private_keys_no_private_keys(self):
        metadata = {
            "name": "Bob Smith",
            "age": 30,
            "location": "New York"
        }

        result = self.processor.remove_private_keys(metadata)

        self.assertEqual(result, metadata)

    def test_remove_private_keys_all_private(self):
        metadata = {
            "_private1": "secret1",
            "_private2": "secret2",
            "_private3": "secret3"
        }

        result = self.processor.remove_private_keys(metadata)

        self.assertEqual(result, {})

    def test_remove_private_keys_empty_dict(self):
        metadata = {}

        result = self.processor.remove_private_keys(metadata)

        self.assertEqual(result, {})

    def test_remove_private_keys_deep_nesting(self):
        metadata = {
            "level1": {
                "level2": {
                    "level3": {
                        "public_data": "visible",
                        "_private_data": "hidden"
                    },
                    "_level2_private": "secret"
                },
                "public_info": "available"
            },
            "_top_level_private": "confidential"
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "level1": {
                "level2": {
                    "level3": {
                        "public_data": "visible"
                    }
                },
                "public_info": "available"
            }
        }

        self.assertEqual(result, expected)

    def test_remove_private_keys_preserves_structure(self):
        metadata = {
            "study": {
                "participants": {
                    "count": 100,
                    "_internal_notes": "classified"
                }
            }
        }

        result = self.processor.remove_private_keys(metadata)

        self.assertIn("study", result)
        self.assertIn("participants", result["study"])
        self.assertEqual(result["study"]["participants"]["count"], 100)
        self.assertNotIn("_internal_notes", result["study"]["participants"])


if __name__ == '__main__':
    unittest.main()