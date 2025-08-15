import pytest
from Pipeline.Transform.MetaDataProcessor import MetaDataProcessor


class TestMetaDataProcessor:
    """Unit tests for the MetaDataProcessor component."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.processor = MetaDataProcessor()

    def test_remove_private_keys_simple(self):
        """Test removal of private keys from simple flat dictionary."""
        metadata = {
            "name": "John Doe",
            "_private_id": "secret123",
            "age": 45,
            "_internal_note": "confidential",
            "location": "Boston"
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "name": "John Doe",
            "age": 45,
            "location": "Boston"
        }

        assert result == expected
        assert "_private_id" not in result
        assert "_internal_note" not in result
        assert len(result) == 3

    def test_remove_private_keys_nested_dictionaries(self):
        """Test removal of private keys from nested dictionary structures."""
        metadata = {
            "participant": {
                "name": "Jane Doe",
                "_ssn": "123-45-6789",
                "contact": {
                    "email": "jane@example.com",
                    "_phone": "555-1234",
                    "_emergency_contact": "John Doe"
                },
                "age": 35
            },
            "_system_id": "sys123",
            "study_id": "DNA-001"
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "participant": {
                "name": "Jane Doe",
                "contact": {
                    "email": "jane@example.com"
                },
                "age": 35
            },
            "study_id": "DNA-001"
        }

        assert result == expected
        assert "_system_id" not in result
        assert "_ssn" not in result["participant"]
        assert "_phone" not in result["participant"]["contact"]
        assert "_emergency_contact" not in result["participant"]["contact"]

    def test_remove_private_keys_no_private_keys(self):
        """Test processing of metadata with no private keys."""
        metadata = {
            "name": "Alice Smith",
            "age": 30,
            "location": "Seattle",
            "contact": {
                "email": "alice@example.com",
                "address": "123 Main St"
            }
        }

        result = self.processor.remove_private_keys(metadata)

        # Should return identical structure when no private keys exist
        assert result == metadata
        assert result is not metadata  # Should be a copy, not the same object

    def test_remove_private_keys_all_private_keys(self):
        """Test processing of metadata containing only private keys."""
        metadata = {
            "_secret1": "value1",
            "_secret2": "value2",
            "_nested": {
                "_private": "data",
                "_more_private": "info"
            }
        }

        result = self.processor.remove_private_keys(metadata)

        assert result == {}
        assert len(result) == 0

    def test_remove_private_keys_empty_dictionary(self):
        """Test processing of empty metadata dictionary."""
        metadata = {}

        result = self.processor.remove_private_keys(metadata)

        assert result == {}
        assert result is not metadata  # Should be a new object

    def test_remove_private_keys_deeply_nested(self):
        """Test removal from deeply nested dictionary structures."""
        metadata = {
            "level1": {
                "level2": {
                    "level3": {
                        "public_data": "visible",
                        "_private_data": "hidden",
                        "level4": {
                            "_deep_secret": "classified",
                            "deep_public": "open"
                        }
                    },
                    "_level2_private": "secret"
                },
                "public_level2": "data"
            },
            "_top_level_private": "hidden"
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "level1": {
                "level2": {
                    "level3": {
                        "public_data": "visible",
                        "level4": {
                            "deep_public": "open"
                        }
                    }
                },
                "public_level2": "data"
            }
        }

        assert result == expected
        assert "_top_level_private" not in result
        assert "_level2_private" not in result["level1"]["level2"]
        assert "_private_data" not in result["level1"]["level2"]["level3"]
        assert "_deep_secret" not in result["level1"]["level2"]["level3"]["level4"]

    def test_remove_private_keys_preserves_data_types(self):
        """Test that non-string data types are preserved correctly."""
        metadata = {
            "age": 42,
            "height": 5.9,
            "is_active": True,
            "scores": [85, 90, 78],
            "_private_score": 95,
            "nested": {
                "count": 10,
                "_private_count": 20,
                "ratio": 0.75
            }
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "age": 42,
            "height": 5.9,
            "is_active": True,
            "scores": [85, 90, 78],
            "nested": {
                "count": 10,
                "ratio": 0.75
            }
        }

        assert result == expected
        assert isinstance(result["age"], int)
        assert isinstance(result["height"], float)
        assert isinstance(result["is_active"], bool)
        assert isinstance(result["scores"], list)
        assert isinstance(result["nested"]["ratio"], float)

    def test_remove_private_keys_mixed_underscore_cases(self):
        """Test handling of various underscore patterns."""
        metadata = {
            "_private": "hidden",           # Should be removed
            "__dunder": "also_hidden",      # Should be removed (starts with _)
            "not_private": "visible",       # Should be kept (underscore not at start)
            "public_": "visible",           # Should be kept (underscore at end)
            "mid_underscore": "visible",    # Should be kept (underscore in middle)
            "_": "single_underscore"        # Should be removed
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "not_private": "visible",
            "public_": "visible",
            "mid_underscore": "visible"
        }

        assert result == expected
        assert "_private" not in result
        assert "__dunder" not in result
        assert "_" not in result

    def test_remove_private_keys_complex_real_world_example(self, sample_metadata_with_private):
        """Test with complex real-world metadata structure."""
        result = self.processor.remove_private_keys(sample_metadata_with_private)

        # Verify all private keys are removed
        result_str = str(result)
        assert "_ssn" not in result_str
        assert "_internal_id" not in result_str
        assert "_phone" not in result_str
        assert "_emergency_contact_ssn" not in result_str
        assert "_medical_record_number" not in result_str
        assert "_system_metadata" not in result_str
        assert "_created_by" not in result_str
        assert "_last_modified" not in result_str

        # Verify public data is preserved
        assert result["participant_id"] == "test-123"
        assert result["name"] == "John Doe"
        assert result["contact"]["email"] == "john@example.com"

    def test_remove_private_keys_does_not_modify_original(self):
        """Test that the original metadata dictionary is not modified."""
        original_metadata = {
            "public": "data",
            "_private": "secret",
            "nested": {
                "public_nested": "visible",
                "_private_nested": "hidden"
            }
        }
        original_copy = original_metadata.copy()

        result = self.processor.remove_private_keys(original_metadata)

        # Original should be unchanged
        assert original_metadata == original_copy
        assert "_private" in original_metadata
        assert "_private_nested" in original_metadata["nested"]

        # Result should be different
        assert result != original_metadata
        assert "_private" not in result
        assert "_private_nested" not in result["nested"]

    def test_remove_private_keys_empty_nested_dictionaries(self):
        """Test handling of nested dictionaries that become empty after cleaning."""
        metadata = {
            "valid_data": "keep",
            "empty_after_cleaning": {
                "_all": "private",
                "_data": "removed"
            },
            "partial_cleaning": {
                "keep_this": "data",
                "_remove_this": "private"
            }
        }

        result = self.processor.remove_private_keys(metadata)

        expected = {
            "valid_data": "keep",
            "empty_after_cleaning": {},  # Empty dict after removing private keys
            "partial_cleaning": {
                "keep_this": "data"
            }
        }

        assert result == expected
        assert "empty_after_cleaning" in result
        assert result["empty_after_cleaning"] == {}

    def test_remove_private_keys_non_string_keys(self):
        """Test that processor handles non-string keys (edge case)."""
        # Since JSON only has string keys, we'll test string keys that look like numbers
        metadata = {
            "string_key": "value",
            "123": "numeric_looking_key",  # String key that looks numeric
            "_private": "secret"
        }

        result = self.processor.remove_private_keys(metadata)

        # Should handle all string keys correctly
        assert "string_key" in result
        assert "123" in result  # String keys should be preserved
        assert "_private" not in result