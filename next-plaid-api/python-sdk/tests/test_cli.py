"""
Unit tests for the Next Plaid CLI.

These tests use Click's CliRunner and mock the SDK client so they run
without a server. They cover helpers, every command, and error paths.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from next_plaid_client.cli import (
    _parse_json_param,
    _parse_params,
    _to_dict,
    cli,
)
from next_plaid_client.exceptions import NextPlaidError
from next_plaid_client.models import (
    EncodeResponse,
    HealthResponse,
    IndexInfo,
    IndexSummary,
    MetadataCheckResponse,
    MetadataResponse,
    QueryResult,
    RerankResponse,
    RerankResult,
    SearchResult,
)


# ──────────────────── helpers ────────────────────


class TestParseParams:
    def test_empty_tuple(self):
        assert _parse_params(()) is None

    def test_integers(self):
        assert _parse_params(("1", "2", "3")) == [1, 2, 3]

    def test_floats(self):
        assert _parse_params(("1.5", "2.7")) == [1.5, 2.7]

    def test_strings(self):
        assert _parse_params(("foo", "bar")) == ["foo", "bar"]

    def test_mixed(self):
        assert _parse_params(("42", "3.14", "hello")) == [42, 3.14, "hello"]

    def test_int_preferred_over_float(self):
        # "10" should parse as int, not float
        result = _parse_params(("10",))
        assert result == [10]
        assert isinstance(result[0], int)


class TestToDict:
    def test_plain_dict(self):
        assert _to_dict({"a": 1}) == {"a": 1}

    def test_plain_list(self):
        assert _to_dict([1, 2]) == [1, 2]

    def test_primitive(self):
        assert _to_dict(42) == 42
        assert _to_dict("hello") == "hello"

    def test_dataclass_like(self):
        obj = MagicMock()
        obj.__dict__ = {"x": 1, "y": "two", "_private": "skip"}
        result = _to_dict(obj)
        assert result == {"x": 1, "y": "two"}

    def test_nested(self):
        inner = MagicMock()
        inner.__dict__ = {"val": 10}
        outer = MagicMock()
        outer.__dict__ = {"child": inner, "items": [1, 2]}
        result = _to_dict(outer)
        assert result == {"child": {"val": 10}, "items": [1, 2]}


class TestParseJsonParam:
    def test_valid_json(self):
        assert _parse_json_param('{"a": 1}', "test") == {"a": 1}

    def test_invalid_json(self):
        with pytest.raises(Exception, match="Invalid JSON"):
            _parse_json_param("not json", "test")


# ──────────────────── fixtures ────────────────────

# Mock return objects used across command tests.


def _health():
    return HealthResponse(
        status="healthy",
        version="1.0.0",
        loaded_indices=1,
        index_dir="/data",
        memory_usage_bytes=1024,
        indices=[
            IndexSummary(
                name="idx",
                num_documents=10,
                num_embeddings=100,
                num_partitions=4,
                dimension=128,
                nbits=4,
                avg_doclen=10.0,
                has_metadata=True,
            )
        ],
    )


def _index_info():
    return IndexInfo(
        name="my_index",
        num_documents=5,
        num_embeddings=50,
        num_partitions=2,
        avg_doclen=10.0,
        dimension=128,
        has_metadata=False,
    )


def _search_result():
    return SearchResult(
        num_queries=1,
        results=[
            QueryResult(
                query_id=0,
                document_ids=[1, 2],
                scores=[0.95, 0.80],
                metadata=None,
            )
        ],
    )


def _search_result_with_metadata():
    return SearchResult(
        num_queries=1,
        results=[
            QueryResult(
                query_id=0,
                document_ids=[1],
                scores=[0.9],
                metadata=[{"cat": "science"}],
            )
        ],
    )


def _encode_response():
    return EncodeResponse(
        embeddings=[[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]],
        num_texts=1,
    )


def _rerank_response():
    return RerankResponse(
        results=[RerankResult(index=1, score=0.9), RerankResult(index=0, score=0.3)],
        num_documents=2,
    )


def _metadata_response():
    return MetadataResponse(metadata=[{"cat": "science"}], count=1)


def _metadata_check_response():
    return MetadataCheckResponse(
        existing_ids=[1, 2], missing_ids=[3], existing_count=2, missing_count=1
    )


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_client():
    """Return a mock that works as a context manager (with _get_client(ctx) as client:)."""
    m = MagicMock()
    m.__enter__ = MagicMock(return_value=m)
    m.__exit__ = MagicMock(return_value=False)
    return m


# ──────────────────── global options ────────────────────


class TestGlobalOptions:
    def test_help(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Next Plaid CLI" in result.output

    def test_custom_url_and_timeout(self, runner, mock_client):
        mock_client.health.return_value = _health()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["-u", "http://remote:9090", "-t", "60", "health"]
            )
        assert result.exit_code == 0

    def test_header_parsing(self, runner, mock_client):
        mock_client.health.return_value = _health()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["-H", "Authorization: Bearer tok", "health"]
            )
        assert result.exit_code == 0

    def test_bad_header(self, runner):
        result = runner.invoke(cli, ["-H", "no-colon", "health"])
        assert result.exit_code != 0
        assert "Key: Value" in result.output


# ──────────────────── health ────────────────────


class TestHealthCommand:
    def test_human_output(self, runner, mock_client):
        mock_client.health.return_value = _health()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["health"])
        assert result.exit_code == 0
        assert "status: healthy" in result.output
        assert "version: 1.0.0" in result.output
        assert "loaded_indices: 1" in result.output
        assert "idx" in result.output

    def test_json_output(self, runner, mock_client):
        mock_client.health.return_value = _health()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["--json", "health"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "healthy"

    def test_error(self, runner, mock_client):
        mock_client.__enter__.side_effect = NextPlaidError(
            "connection refused", code="CONN"
        )
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["health"])
        assert result.exit_code != 0
        assert "connection refused" in result.output


# ──────────────────── index ────────────────────


class TestIndexCommands:
    def test_list_human(self, runner, mock_client):
        mock_client.list_indices.return_value = ["idx_a", "idx_b"]
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "list"])
        assert result.exit_code == 0
        assert "idx_a" in result.output
        assert "idx_b" in result.output

    def test_list_empty(self, runner, mock_client):
        mock_client.list_indices.return_value = []
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "list"])
        assert result.exit_code == 0
        assert "(no indices)" in result.output

    def test_list_json(self, runner, mock_client):
        mock_client.list_indices.return_value = ["idx_a"]
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["--json", "index", "list"])
        assert result.exit_code == 0
        assert json.loads(result.output) == ["idx_a"]

    def test_get(self, runner, mock_client):
        mock_client.get_index.return_value = _index_info()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "get", "my_index"])
        assert result.exit_code == 0
        assert "name: my_index" in result.output
        assert "documents: 5" in result.output

    def test_get_with_optional_fields(self, runner, mock_client):
        info = _index_info()
        info.metadata_count = 3
        info.max_documents = 1000
        mock_client.get_index.return_value = info
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "get", "my_index"])
        assert "metadata_count: 3" in result.output
        assert "max_documents: 1000" in result.output

    def test_create(self, runner, mock_client):
        mock_client.create_index.return_value = {"status": "ok"}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "create", "new_idx"])
        assert result.exit_code == 0
        assert "created index: new_idx" in result.output
        mock_client.create_index.assert_called_once()

    def test_create_with_options(self, runner, mock_client):
        mock_client.create_index.return_value = {"status": "ok"}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "index", "create", "new_idx",
                    "--nbits", "2",
                    "--batch-size", "100",
                    "--seed", "42",
                    "--max-documents", "5000",
                    "--fts-tokenizer", "trigram",
                ],
            )
        assert result.exit_code == 0
        config = mock_client.create_index.call_args[0][1]
        assert config.nbits == 2
        assert config.batch_size == 100
        assert config.seed == 42
        assert config.max_documents == 5000
        assert config.fts_tokenizer == "trigram"

    def test_delete_dry_run(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "delete", "old_idx", "--dry-run"])
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "old_idx" in result.output
        mock_client.delete_index.assert_not_called()

    def test_delete_with_yes(self, runner, mock_client):
        mock_client.delete_index.return_value = {"status": "ok"}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "delete", "old_idx", "--yes"])
        assert result.exit_code == 0
        assert "deleted index: old_idx" in result.output

    def test_delete_aborted(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["index", "delete", "old_idx"], input="n\n")
        assert result.exit_code != 0
        mock_client.delete_index.assert_not_called()

    def test_config(self, runner, mock_client):
        mock_client.update_index_config.return_value = {"status": "ok"}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["index", "config", "my_idx", "--max-documents", "1000"]
            )
        assert result.exit_code == 0
        mock_client.update_index_config.assert_called_once_with("my_idx", 1000)

    def test_config_zero_removes_limit(self, runner, mock_client):
        mock_client.update_index_config.return_value = {"status": "ok"}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["index", "config", "my_idx", "--max-documents", "0"]
            )
        assert result.exit_code == 0
        mock_client.update_index_config.assert_called_once_with("my_idx", None)


# ──────────────────── document ────────────────────


class TestDocumentCommands:
    def test_add_text(self, runner, mock_client):
        mock_client.add.return_value = "queued"
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["document", "add", "my_idx", "--text", "Hello", "--text", "World"],
            )
        assert result.exit_code == 0
        assert "queued 2 document(s)" in result.output
        mock_client.add.assert_called_once_with(
            "my_idx", ["Hello", "World"], metadata=None, pool_factor=None
        )

    def test_add_from_file(self, runner, mock_client):
        mock_client.add.return_value = "queued"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(["doc one", "doc two"], f)
            f.flush()
            fpath = f.name
        try:
            with patch("next_plaid_client.cli._get_client", return_value=mock_client):
                result = runner.invoke(
                    cli, ["document", "add", "my_idx", "--file", fpath]
                )
            assert result.exit_code == 0
            assert "queued 2 document(s)" in result.output
        finally:
            os.unlink(fpath)

    def test_add_from_stdin(self, runner, mock_client):
        mock_client.add.return_value = "queued"
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["document", "add", "my_idx", "--stdin"],
                input='["one", "two", "three"]',
            )
        assert result.exit_code == 0
        assert "queued 3 document(s)" in result.output

    def test_add_with_metadata(self, runner, mock_client):
        mock_client.add.return_value = "queued"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump([{"cat": "a"}], f)
            f.flush()
            fpath = f.name
        try:
            with patch("next_plaid_client.cli._get_client", return_value=mock_client):
                result = runner.invoke(
                    cli,
                    [
                        "document", "add", "my_idx",
                        "--text", "Doc",
                        "--metadata-file", fpath,
                    ],
                )
            assert result.exit_code == 0
            mock_client.add.assert_called_once_with(
                "my_idx", ["Doc"], metadata=[{"cat": "a"}], pool_factor=None
            )
        finally:
            os.unlink(fpath)

    def test_add_with_pool_factor(self, runner, mock_client):
        mock_client.add.return_value = "queued"
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["document", "add", "my_idx", "--text", "Doc", "--pool-factor", "2"],
            )
        assert result.exit_code == 0
        mock_client.add.assert_called_once_with(
            "my_idx", ["Doc"], metadata=None, pool_factor=2
        )

    def test_add_no_input(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["document", "add", "my_idx"])
        assert result.exit_code != 0
        assert "No documents provided" in result.output

    def test_add_json_output(self, runner, mock_client):
        mock_client.add.return_value = "queued"
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["--json", "document", "add", "my_idx", "--text", "Doc"]
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["count"] == 1
        assert data["index"] == "my_idx"

    def test_delete_dry_run(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "document", "delete", "my_idx",
                    "-c", "year < ?",
                    "-p", "2020",
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "year < ?" in result.output
        assert "2020" in result.output
        mock_client.delete.assert_not_called()

    def test_delete_with_yes(self, runner, mock_client):
        mock_client.delete.return_value = {"status": "ok"}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "document", "delete", "my_idx",
                    "-c", "cat = ?",
                    "-p", "draft",
                    "--yes",
                ],
            )
        assert result.exit_code == 0
        assert "delete queued" in result.output
        mock_client.delete.assert_called_once_with(
            "my_idx", "cat = ?", parameters=["draft"]
        )

    def test_delete_aborted(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["document", "delete", "my_idx", "-c", "id = ?", "-p", "1"],
                input="n\n",
            )
        assert result.exit_code != 0
        mock_client.delete.assert_not_called()


# ──────────────────── search ────────────────────


class TestSearchCommand:
    def test_semantic_search(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["search", "my_idx", "What is AI?"])
        assert result.exit_code == 0
        assert "num_queries: 1" in result.output
        assert "doc_id=1" in result.output
        assert "0.9500" in result.output

    def test_semantic_search_json(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["--json", "search", "my_idx", "What is AI?"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["num_queries"] == 1

    def test_multi_query(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["search", "my_idx", "q1", "q2"])
        assert result.exit_code == 0
        mock_client.search.assert_called_once()
        call_args = mock_client.search.call_args
        assert call_args[0][1] == ["q1", "q2"]

    def test_keyword_search(self, runner, mock_client):
        mock_client.keyword_search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["search", "my_idx", "--text-query", "machine learning"]
            )
        assert result.exit_code == 0
        mock_client.keyword_search.assert_called_once()
        mock_client.search.assert_not_called()

    def test_hybrid_search(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "search", "my_idx", "What is ML?",
                    "--text-query", "machine learning",
                    "--alpha", "0.75",
                    "--fusion", "rrf",
                ],
            )
        assert result.exit_code == 0
        call_kwargs = mock_client.search.call_args[1]
        assert call_kwargs["text_query"] == ["machine learning"]
        assert call_kwargs["alpha"] == 0.75
        assert call_kwargs["fusion"] == "rrf"

    def test_search_with_filter(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "search", "my_idx", "query",
                    "--filter", "year > ?",
                    "--filter-param", "2020",
                ],
            )
        assert result.exit_code == 0
        call_kwargs = mock_client.search.call_args[1]
        assert call_kwargs["filter_condition"] == "year > ?"
        assert call_kwargs["filter_parameters"] == [2020]

    def test_search_with_subset(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["search", "my_idx", "query", "--subset", "1", "--subset", "5"],
            )
        assert result.exit_code == 0
        call_kwargs = mock_client.search.call_args[1]
        assert call_kwargs["subset"] == [1, 5]

    def test_search_params_forwarded(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "search", "my_idx", "query",
                    "--top-k", "5",
                    "--n-ivf-probe", "16",
                    "--n-full-scores", "2048",
                    "--centroid-threshold", "0.5",
                ],
            )
        assert result.exit_code == 0
        params = mock_client.search.call_args[1]["params"]
        assert params.top_k == 5
        assert params.n_ivf_probe == 16
        assert params.n_full_scores == 2048
        assert params.centroid_score_threshold == 0.5

    def test_centroid_threshold_zero_disables(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["search", "my_idx", "query", "--centroid-threshold", "0"]
            )
        assert result.exit_code == 0
        params = mock_client.search.call_args[1]["params"]
        assert params.centroid_score_threshold is None

    def test_search_from_stdin(self, runner, mock_client):
        mock_client.search.return_value = _search_result()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["search", "my_idx", "--stdin"],
                input='["q1", "q2"]',
            )
        assert result.exit_code == 0
        assert mock_client.search.call_args[0][1] == ["q1", "q2"]

    def test_search_no_queries(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["search", "my_idx"])
        assert result.exit_code != 0
        assert "No queries provided" in result.output

    def test_search_with_metadata_in_results(self, runner, mock_client):
        mock_client.search.return_value = _search_result_with_metadata()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["search", "my_idx", "query"])
        assert result.exit_code == 0
        assert "metadata[0]" in result.output
        assert "science" in result.output

    def test_search_error(self, runner, mock_client):
        mock_client.__enter__.side_effect = NextPlaidError(
            "index not found", code="NOT_FOUND", details="my_idx"
        )
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["search", "my_idx", "query"])
        assert result.exit_code != 0
        assert "index not found" in result.output
        assert "NOT_FOUND" in result.output
        assert "my_idx" in result.output


# ──────────────────── metadata ────────────────────


class TestMetadataCommands:
    def test_list(self, runner, mock_client):
        mock_client.get_metadata.return_value = _metadata_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["metadata", "list", "my_idx"])
        assert result.exit_code == 0
        assert "count: 1" in result.output
        assert "science" in result.output

    def test_count(self, runner, mock_client):
        mock_client.get_metadata_count.return_value = {
            "count": 42,
            "has_metadata": True,
        }
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["metadata", "count", "my_idx"])
        assert result.exit_code == 0
        assert "count: 42" in result.output
        assert "has_metadata: True" in result.output

    def test_check(self, runner, mock_client):
        mock_client.check_metadata.return_value = _metadata_check_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["metadata", "check", "my_idx", "--ids", "1", "--ids", "2", "--ids", "3"]
            )
        assert result.exit_code == 0
        assert "existing" in result.output
        assert "missing" in result.output

    def test_query(self, runner, mock_client):
        mock_client.query_metadata.return_value = {
            "document_ids": [1, 2],
            "count": 2,
        }
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["metadata", "query", "my_idx", "-c", "cat = ?", "-p", "science"],
            )
        assert result.exit_code == 0
        mock_client.query_metadata.assert_called_once_with(
            "my_idx", "cat = ?", parameters=["science"]
        )

    def test_get_by_ids(self, runner, mock_client):
        mock_client.get_metadata_by_ids.return_value = _metadata_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["metadata", "get", "my_idx", "--ids", "1", "--ids", "2"]
            )
        assert result.exit_code == 0
        call_kwargs = mock_client.get_metadata_by_ids.call_args[1]
        assert call_kwargs["document_ids"] == [1, 2]

    def test_get_by_condition(self, runner, mock_client):
        mock_client.get_metadata_by_ids.return_value = _metadata_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "metadata", "get", "my_idx",
                    "-c", "score > ?",
                    "-p", "90",
                    "--limit", "10",
                ],
            )
        assert result.exit_code == 0
        call_kwargs = mock_client.get_metadata_by_ids.call_args[1]
        assert call_kwargs["condition"] == "score > ?"
        assert call_kwargs["parameters"] == [90]
        assert call_kwargs["limit"] == 10

    def test_update_dry_run(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "metadata", "update", "my_idx",
                    "-c", "status = ?",
                    "-p", "draft",
                    "--set", '{"status": "published"}',
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0
        assert "dry-run" in result.output
        assert "status = ?" in result.output
        assert "published" in result.output
        mock_client.update_metadata.assert_not_called()

    def test_update_with_yes(self, runner, mock_client):
        mock_client.update_metadata.return_value = {"updated": 3}
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "metadata", "update", "my_idx",
                    "-c", "status = ?",
                    "-p", "draft",
                    "--set", '{"status": "published"}',
                    "--yes",
                ],
            )
        assert result.exit_code == 0
        assert "updated: 3 row(s)" in result.output

    def test_update_aborted(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "metadata", "update", "my_idx",
                    "-c", "status = ?",
                    "--set", '{"status": "done"}',
                ],
                input="n\n",
            )
        assert result.exit_code != 0
        mock_client.update_metadata.assert_not_called()

    def test_update_bad_json(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                [
                    "metadata", "update", "my_idx",
                    "-c", "x = ?",
                    "--set", "not-json",
                    "--yes",
                ],
            )
        assert result.exit_code != 0
        assert "Invalid JSON" in result.output


# ──────────────────── encode ────────────────────


class TestEncodeCommand:
    def test_encode_texts(self, runner, mock_client):
        mock_client.encode.return_value = _encode_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["encode", "Hello", "World"])
        assert result.exit_code == 0
        assert "num_texts: 1" in result.output
        assert "tokens" in result.output

    def test_encode_json(self, runner, mock_client):
        mock_client.encode.return_value = _encode_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["--json", "encode", "Hello"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["num_texts"] == 1

    def test_encode_input_type(self, runner, mock_client):
        mock_client.encode.return_value = _encode_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["encode", "--input-type", "query", "What is AI?"]
            )
        assert result.exit_code == 0
        mock_client.encode.assert_called_once_with(
            ["What is AI?"], input_type="query", pool_factor=None
        )

    def test_encode_from_stdin(self, runner, mock_client):
        mock_client.encode.return_value = _encode_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["encode", "--stdin"], input='["text1", "text2"]'
            )
        assert result.exit_code == 0
        mock_client.encode.assert_called_once_with(
            ["text1", "text2"], input_type="document", pool_factor=None
        )

    def test_encode_no_input(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["encode"])
        assert result.exit_code != 0
        assert "No texts provided" in result.output

    def test_encode_with_pool_factor(self, runner, mock_client):
        mock_client.encode.return_value = _encode_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["encode", "--pool-factor", "2", "Hello"]
            )
        assert result.exit_code == 0
        mock_client.encode.assert_called_once_with(
            ["Hello"], input_type="document", pool_factor=2
        )


# ──────────────────── rerank ────────────────────


class TestRerankCommand:
    def test_rerank_documents(self, runner, mock_client):
        mock_client.rerank.return_value = _rerank_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["rerank", "-q", "capital of France", "-d", "Paris", "-d", "Berlin"],
            )
        assert result.exit_code == 0
        assert "num_documents: 2" in result.output
        assert "0.9000" in result.output

    def test_rerank_json(self, runner, mock_client):
        mock_client.rerank.return_value = _rerank_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["--json", "rerank", "-q", "query", "-d", "doc"]
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["num_documents"] == 2

    def test_rerank_from_file(self, runner, mock_client):
        mock_client.rerank.return_value = _rerank_response()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(["doc one", "doc two"], f)
            f.flush()
            fpath = f.name
        try:
            with patch("next_plaid_client.cli._get_client", return_value=mock_client):
                result = runner.invoke(
                    cli, ["rerank", "-q", "query", "--file", fpath]
                )
            assert result.exit_code == 0
            mock_client.rerank.assert_called_once_with(
                "query", ["doc one", "doc two"], pool_factor=None
            )
        finally:
            os.unlink(fpath)

    def test_rerank_from_stdin(self, runner, mock_client):
        mock_client.rerank.return_value = _rerank_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli,
                ["rerank", "-q", "query", "--stdin"],
                input='["doc1", "doc2"]',
            )
        assert result.exit_code == 0
        mock_client.rerank.assert_called_once_with(
            "query", ["doc1", "doc2"], pool_factor=None
        )

    def test_rerank_with_pool_factor(self, runner, mock_client):
        mock_client.rerank.return_value = _rerank_response()
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(
                cli, ["rerank", "-q", "query", "-d", "doc", "--pool-factor", "3"]
            )
        assert result.exit_code == 0
        mock_client.rerank.assert_called_once_with("query", ["doc"], pool_factor=3)

    def test_rerank_no_documents(self, runner, mock_client):
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["rerank", "-q", "query"])
        assert result.exit_code != 0
        assert "No documents provided" in result.output

    def test_rerank_missing_query(self, runner, mock_client):
        result = runner.invoke(cli, ["rerank", "-d", "doc"])
        assert result.exit_code != 0


# ──────────────────── error handling ────────────────────


class TestErrorHandling:
    def test_error_with_code_and_details(self, runner, mock_client):
        mock_client.__enter__.side_effect = NextPlaidError(
            "bad request", code="VALIDATION", details="field 'x' missing"
        )
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["health"])
        assert result.exit_code != 0
        assert "bad request" in result.output
        assert "VALIDATION" in result.output
        assert "field 'x' missing" in result.output

    def test_error_without_optional_fields(self, runner, mock_client):
        mock_client.__enter__.side_effect = NextPlaidError("timeout")
        with patch("next_plaid_client.cli._get_client", return_value=mock_client):
            result = runner.invoke(cli, ["health"])
        assert result.exit_code != 0
        assert "timeout" in result.output
