from unittest.mock import patch

import pytest
from braid.errors import PayloadTooLargeError
from braid.serialization import serialize_fn, serialize_input


class TestSerializeFn:
    def test_roundtrip(self):
        def fn(x):
            return x**2

        data = serialize_fn(fn)
        import cloudpickle

        assert cloudpickle.loads(data)(5) == 25

    def test_too_large(self):
        with patch("cloudpickle.dumps") as mock_dumps:
            mock_dumps.return_value = b"x" * (201 * 1024)

            def fn(x):
                return x

            with pytest.raises(PayloadTooLargeError):
                serialize_fn(fn)

    def test_exactly_at_limit(self):
        with patch("cloudpickle.dumps") as mock_dumps:
            mock_dumps.return_value = b"x" * (200 * 1024)

            def fn(x):
                return x

            result = serialize_fn(fn)
            assert len(result) == 200 * 1024


class TestSerializeInput:
    def test_roundtrip(self):
        import cloudpickle

        data = [1, 2, 3]
        assert cloudpickle.loads(serialize_input(data)) == data


class TestPayloadSizeCheck:
    """Test the payload size check for Lambda invoke."""

    def test_payload_too_large_error_with_what_parameter(self):
        """PayloadTooLargeError should accept optional 'what' parameter."""
        err = PayloadTooLargeError(300 * 1024, 250 * 1024, what="batch inputs")
        assert "batch inputs" in str(err)
        assert "300.0KB" in str(err)
        assert "250.0KB" in str(err)
        assert "reducing batch_size" in str(err) or "batch_size" in str(err)

    def test_payload_too_large_error_default_message(self):
        """PayloadTooLargeError should default to 'function' for backward compatibility."""
        err = PayloadTooLargeError(201 * 1024, 200 * 1024)
        assert "function" in str(err).lower()
        assert "201.0KB" in str(err)
        assert "200.0KB" in str(err)
