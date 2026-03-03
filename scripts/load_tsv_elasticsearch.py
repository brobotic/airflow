#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, TextIO, Tuple

from elasticsearch import Elasticsearch, helpers
from loguru import logger

if TYPE_CHECKING:
	from confluent_kafka import Producer

def configure_csv_field_size_limit() -> None:
	max_int = sys.maxsize
	while True:
		try:
			csv.field_size_limit(max_int)
			return
		except OverflowError:
			max_int = max_int // 10


@dataclass
class FileReport:
	file_path: Path
	target_name: str
	rows: int
	indexed: int
	errors: int
	duration_seconds: float
	status: str
	message: str = ""


def configure_logging(log_level: str, log_file: str | None = None) -> None:
	logger.remove()
	log_format = (
		"<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
		"<level>{level:<8}</level> | "
		"<level>{message}</level>"
	)
	logger.add(
		sys.stderr,
		level=log_level.upper(),
		format=log_format,
		colorize=True,
	)
	if log_file:
		logger.add(
			log_file,
			level=log_level.upper(),
			format=log_format,
			enqueue=True,
		)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Load TSV files from a directory into Elasticsearch or Kafka"
	)
	parser.add_argument(
		"--output",
		choices=["elasticsearch", "kafka"],
		default="elasticsearch",
		help="Output target type (default: elasticsearch)",
	)
	parser.add_argument(
		"--host",
		default="http://localhost:9200",
		help="Elasticsearch host URL (default: http://localhost:9200)",
	)
	parser.add_argument(
		"--api-key",
		default=None,
		help="Elasticsearch API key (optional)",
	)
	parser.add_argument(
		"--username",
		default=None,
		help="Elasticsearch username (optional)",
	)
	parser.add_argument(
		"--password",
		default=None,
		help="Elasticsearch password (optional)",
	)
	parser.add_argument(
		"--kafka-bootstrap-servers",
		default=None,
		help="Kafka bootstrap servers, e.g. localhost:9092 (required when --output kafka)",
	)
	parser.add_argument(
		"--kafka-topic",
		default=None,
		help="Kafka topic name (required when --output kafka)",
	)
	parser.add_argument(
		"--kafka-client-id",
		default="load-tsv",
		help="Kafka producer client.id (default: load-tsv)",
	)
	parser.add_argument(
		"--kafka-acks",
		default="all",
		help="Kafka producer acks setting (default: all)",
	)
	parser.add_argument(
		"--kafka-linger-ms",
		type=int,
		default=50,
		help="Kafka producer linger.ms (default: 50)",
	)
	parser.add_argument(
		"--kafka-batch-size",
		type=int,
		default=131072,
		help="Kafka producer batch.size in bytes (default: 131072)",
	)
	parser.add_argument(
		"--kafka-compression-type",
		default="none",
		choices=["none", "gzip", "snappy", "lz4", "zstd"],
		help="Kafka compression codec (default: none)",
	)
	parser.add_argument(
		"--kafka-key-column",
		default=None,
		help="Optional TSV column to use as Kafka message key",
	)
	parser.add_argument(
		"--input-dir",
		default="datasets",
		help="Directory containing TSV files (default: datasets)",
	)
	parser.add_argument(
		"--pattern",
		default="*.tsv",
		help="Glob pattern for TSV files (default: *.tsv)",
	)
	parser.add_argument(
		"--index-prefix",
		default="movies",
		help="Prefix for generated index names (default: movies)",
	)
	parser.add_argument(
		"--id-column",
		default=None,
		help="Optional column to use as document _id",
	)
	parser.add_argument(
		"--chunk-size",
		type=int,
		default=1000,
		help="Bulk index chunk size (default: 1000)",
	)
	parser.add_argument(
		"--max-chunk-bytes",
		type=int,
		default=20 * 1024 * 1024,
		help="Maximum bulk request size in bytes (default: 20971520)",
	)
	parser.add_argument(
		"--parallel-workers",
		type=int,
		default=8,
		help="Number of parallel bulk worker threads for Elasticsearch (default: 8)",
	)
	parser.add_argument(
		"--op-type",
		choices=["index", "create"],
		default="index",
		help="Elasticsearch bulk op type (default: index)",
	)
	parser.add_argument(
		"--fast-index-mode",
		action="store_true",
		help=(
			"Temporarily set refresh_interval=-1 and number_of_replicas=0 during load, "
			"then restore original settings"
		),
	)
	parser.add_argument(
		"--create-index",
		action="store_true",
		help="Create indices before loading if they do not exist",
	)
	parser.add_argument(
		"--create-index-shards",
		type=int,
		default=None,
		help="Primary shard count to use when creating indices (optional)",
	)
	parser.add_argument(
		"--create-index-replicas",
		type=int,
		default=None,
		help="Replica count to use when creating indices (optional)",
	)
	parser.add_argument(
		"--create-index-refresh-interval",
		default=None,
		help='Refresh interval to use when creating indices, e.g. "1s" or "-1" (optional)',
	)
	parser.add_argument(
		"--overwrite-index",
		action="store_true",
		help="Delete existing index before creating it",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Read TSV files and print stats without indexing",
	)
	parser.add_argument(
		"--log-level",
		default="INFO",
		choices=["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"],
		help="Logging verbosity level (default: INFO)",
	)
	parser.add_argument(
		"--log-file",
		default=None,
		help="Optional file path to write logs (in addition to console output)",
	)
	parser.add_argument(
		"--error-log-file",
		default=None,
		help=(
			"Optional path to write failed rows as NDJSON with row_number, "
			"document_id, and Elasticsearch error details"
		),
	)
	parser.add_argument(
		"--error-sample-limit",
		type=int,
		default=10,
		help="Number of failed rows to show inline per file (default: 10)",
	)
	return parser.parse_args()


def clean_index_name(name: str) -> str:
	cleaned = re.sub(r"[^a-z0-9._-]+", "-", name.lower()).strip("-._")
	return cleaned[:255] if cleaned else "dataset"


def normalize_value(value: str) -> Any:
	if value is None:
		return None

	trimmed = value.strip()
	if trimmed == "" or trimmed == "\\N":
		return None

	lower = trimmed.lower()
	if lower in {"true", "false"}:
		return lower == "true"

	if re.fullmatch(r"-?\d+", trimmed):
		try:
			return int(trimmed)
		except ValueError:
			return trimmed

	if re.fullmatch(r"-?(?:\d+\.\d*|\d*\.\d+)", trimmed):
		try:
			return float(trimmed)
		except ValueError:
			return trimmed

	if (trimmed.startswith("{") and trimmed.endswith("}")) or (
		trimmed.startswith("[") and trimmed.endswith("]")
	):
		try:
			return json.loads(trimmed)
		except json.JSONDecodeError:
			return trimmed

	return trimmed


def row_to_doc(row: Dict[str, str]) -> Dict[str, Any]:
	return {key: normalize_value(value) for key, value in row.items()}


def yield_actions(
	file_path: Path,
	index_name: str,
	id_column: str | None = None,
) -> Iterable[Dict[str, Any]]:
	with file_path.open("r", encoding="utf-8", newline="") as handle:
		reader = csv.DictReader(handle, delimiter="\t")
		if reader.fieldnames is None:
			raise ValueError(f"TSV file has no header row: {file_path}")

		for row in reader:
			source = row_to_doc(row)
			action: Dict[str, Any] = {"_index": index_name, "_source": source}
			if id_column and id_column in row and row[id_column] not in (None, "", "\\N"):
				action["_id"] = str(row[id_column])
			yield action


def count_rows(file_path: Path) -> int:
	with file_path.open("r", encoding="utf-8", newline="") as handle:
		reader = csv.reader(handle, delimiter="\t")
		next(reader, None)
		return sum(1 for _ in reader)


def create_client(args: argparse.Namespace) -> Elasticsearch:
	auth_kwargs: Dict[str, Any] = {}
	if args.api_key:
		auth_kwargs["api_key"] = args.api_key
	elif args.username and args.password:
		auth_kwargs["basic_auth"] = (args.username, args.password)

	client = Elasticsearch(args.host, **auth_kwargs)
	if not client.ping():
		raise ConnectionError(f"Unable to connect to Elasticsearch at {args.host}")
	return client


def create_kafka_producer(args: argparse.Namespace) -> "Producer":
	if not args.kafka_bootstrap_servers:
		raise ValueError("--kafka-bootstrap-servers is required when --output kafka")
	if not args.kafka_topic:
		raise ValueError("--kafka-topic is required when --output kafka")

	try:
		from confluent_kafka import Producer
	except ModuleNotFoundError as exc:
		raise ModuleNotFoundError(
			"Kafka output requires confluent-kafka. Install dependencies with: pip install -r requirements.txt"
		) from exc

	producer_config: Dict[str, Any] = {
		"bootstrap.servers": args.kafka_bootstrap_servers,
		"client.id": args.kafka_client_id,
		"acks": args.kafka_acks,
		"linger.ms": args.kafka_linger_ms,
		"batch.size": args.kafka_batch_size,
	}
	if args.kafka_compression_type != "none":
		producer_config["compression.type"] = args.kafka_compression_type

	producer = Producer(producer_config)
	metadata = producer.list_topics(timeout=10)
	if metadata is None or metadata.topics is None:
		raise ConnectionError("Unable to fetch Kafka metadata from bootstrap servers")
	return producer


def maybe_prepare_index(
	client: Elasticsearch,
	index_name: str,
	create_index: bool,
	overwrite_index: bool,
	create_index_shards: int | None,
	create_index_replicas: int | None,
	create_index_refresh_interval: str | None,
) -> None:
	exists = client.indices.exists(index=index_name)
	if overwrite_index and exists:
		client.indices.delete(index=index_name)
		exists = False
		logger.info(f"Deleted existing index: {index_name}")

	if create_index and not exists:
		settings: Dict[str, Any] = {}
		if create_index_shards is not None:
			settings["number_of_shards"] = create_index_shards
		if create_index_replicas is not None:
			settings["number_of_replicas"] = create_index_replicas
		if create_index_refresh_interval is not None:
			settings["refresh_interval"] = create_index_refresh_interval

		body: Dict[str, Any] = {"settings": settings} if settings else {}
		client.indices.create(index=index_name, **body)
		logger.info(f"Created index: {index_name}")


def extract_bulk_error_reason(item: Dict[str, Any]) -> str:
	if "exception" in item:
		return str(item.get("exception"))

	op_type, payload = next(iter(item.items()))
	error = payload.get("error")
	if isinstance(error, dict):
		reason = error.get("reason")
		error_type = error.get("type")
		if error_type and reason:
			return f"{op_type}/{error_type}: {reason}"
		if reason:
			return f"{op_type}: {reason}"
	if error is not None:
		return f"{op_type}: {error}"
	status = payload.get("status")
	if status is not None:
		return f"{op_type}: status={status}"
	return f"{op_type}: unknown bulk error"


def yield_index_actions_with_metadata(
	file_path: Path,
	index_name: str,
	id_column: str | None = None,
	op_type: str = "index",
	) -> Iterable[Dict[str, Any]]:
	with file_path.open("r", encoding="utf-8", newline="") as handle:
		reader = csv.DictReader(handle, delimiter="\t")
		if reader.fieldnames is None:
			raise ValueError(f"TSV file has no header row: {file_path}")

		for row in reader:
			source = row_to_doc(row)
			action: Dict[str, Any] = {
				"_index": index_name,
				"_source": source,
				"_op_type": op_type,
			}
			if id_column and id_column in row and row[id_column] not in (None, "", "\\N"):
				action["_id"] = str(row[id_column])
			yield action


def capture_index_settings(client: Elasticsearch, index_name: str) -> Dict[str, str]:
	settings_response = client.indices.get_settings(index=index_name)
	index_settings = settings_response.get(index_name, {}).get("settings", {}).get("index", {})
	return {
		"number_of_replicas": str(index_settings.get("number_of_replicas", "1")),
		"refresh_interval": str(index_settings.get("refresh_interval", "1s")),
	}


def apply_fast_index_mode(client: Elasticsearch, index_name: str) -> Dict[str, str]:
	original_settings = capture_index_settings(client=client, index_name=index_name)
	client.indices.put_settings(
		index=index_name,
		settings={
			"index": {
				"number_of_replicas": "0",
				"refresh_interval": "-1",
			}
		},
	)
	logger.info(
		"Applied fast-index-mode to "
		f"{index_name} (number_of_replicas=0, refresh_interval=-1)"
	)
	return original_settings


def restore_index_settings(
	client: Elasticsearch,
	index_name: str,
	original_settings: Dict[str, str],
) -> None:
	client.indices.put_settings(
		index=index_name,
		settings={
			"index": {
				"number_of_replicas": original_settings.get("number_of_replicas", "1"),
				"refresh_interval": original_settings.get("refresh_interval", "1s"),
			}
		},
	)
	logger.info(
		"Restored index settings for "
		f"{index_name} (number_of_replicas={original_settings.get('number_of_replicas', '1')}, "
		f"refresh_interval={original_settings.get('refresh_interval', '1s')})"
	)


def bulk_load_file(
	client: Elasticsearch,
	file_path: Path,
	index_name: str,
	id_column: str | None,
	chunk_size: int,
	max_chunk_bytes: int,
	parallel_workers: int,
	op_type: str,
	error_log_handle: TextIO | None,
	error_sample_limit: int,
) -> Tuple[int, int]:
	failure_samples: List[str] = []
	success_count = 0
	error_count = 0

	actions = yield_index_actions_with_metadata(
		file_path=file_path,
		index_name=index_name,
		id_column=id_column,
		op_type=op_type,
	)

	if parallel_workers > 1:
		bulk_results = helpers.parallel_bulk(
			client,
			actions,
			chunk_size=chunk_size,
			max_chunk_bytes=max_chunk_bytes,
			thread_count=parallel_workers,
			queue_size=max(parallel_workers * 4, 8),
			raise_on_error=False,
			raise_on_exception=False,
		)
	else:
		bulk_results = helpers.streaming_bulk(
			client,
			actions,
			chunk_size=chunk_size,
			max_chunk_bytes=max_chunk_bytes,
			raise_on_error=False,
			raise_on_exception=False,
		)

	for ok, result in bulk_results:
		if ok:
			success_count += 1
			continue

		error_count += 1
		error_reason = extract_bulk_error_reason(result)
		op_type_name, payload = next(iter(result.items()))
		document_id = payload.get("_id") if isinstance(payload, dict) else None
		sample = f"op={op_type_name}, id={document_id}, reason={error_reason}"
		if len(failure_samples) < max(error_sample_limit, 0):
			failure_samples.append(sample)

		if error_log_handle is not None:
			error_record = {
				"file": str(file_path),
				"index": index_name,
				"document_id": document_id,
				"error_reason": error_reason,
				"bulk_result": result,
			}
			error_log_handle.write(json.dumps(error_record, ensure_ascii=False) + "\n")

	if failure_samples:
		for sample in failure_samples:
			logger.warning(f"Failed row sample in {file_path.name}: {sample}")

	if error_count > len(failure_samples):
		remaining = error_count - len(failure_samples)
		logger.warning(
			f"{file_path.name}: {remaining} additional failed rows not shown inline"
		)

	return success_count, error_count


def produce_file_to_kafka(
	producer: "Producer",
	file_path: Path,
	topic: str,
	key_column: str | None,
	chunk_size: int,
) -> Tuple[int, int]:
	result = {"success": 0, "errors": 0}

	def delivery_report(err: Any, _msg: Any) -> None:
		if err is not None:
			result["errors"] += 1
		else:
			result["success"] += 1

	with file_path.open("r", encoding="utf-8", newline="") as handle:
		reader = csv.DictReader(handle, delimiter="\t")
		if reader.fieldnames is None:
			raise ValueError(f"TSV file has no header row: {file_path}")

		queued = 0
		for row in reader:
			source = row_to_doc(row)
			payload = json.dumps(source, ensure_ascii=False).encode("utf-8")
			key: bytes | None = None
			if key_column and key_column in row and row[key_column] not in (None, "", "\\N"):
				key = str(row[key_column]).encode("utf-8")

			while True:
				try:
					producer.produce(topic=topic, key=key, value=payload, on_delivery=delivery_report)
					break
				except BufferError:
					producer.poll(0.5)

			queued += 1
			producer.poll(0)
			if queued % chunk_size == 0:
				producer.flush()

	producer.flush()
	return result["success"], result["errors"]


def list_tsv_files(input_dir: Path, pattern: str) -> List[Path]:
	if not input_dir.exists() or not input_dir.is_dir():
		raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

	files = sorted(path for path in input_dir.glob(pattern) if path.is_file())
	if not files:
		raise FileNotFoundError(
			f"No files found in {input_dir} matching pattern '{pattern}'"
		)
	return files


def format_duration(seconds: float) -> str:
	if seconds < 60:
		return f"{seconds:.2f}s"
	minutes, rem = divmod(seconds, 60)
	if minutes < 60:
		return f"{int(minutes)}m {rem:.2f}s"
	hours, rem_minutes = divmod(minutes, 60)
	return f"{int(hours)}h {int(rem_minutes)}m {rem:.2f}s"


def print_summary(
	mode: str,
	reports: List[FileReport],
	total_elapsed_seconds: float,
) -> None:
	files_total = len(reports)
	files_ok = sum(1 for item in reports if item.status in {"dry-run", "ok"})
	files_failed = files_total - files_ok
	total_rows = sum(item.rows for item in reports)
	total_indexed = sum(item.indexed for item in reports)
	total_errors = sum(item.errors for item in reports)

	logger.info("=== Run Summary ===")
	logger.info(f"Mode: {mode}")
	logger.info(f"Files: {files_total} (ok={files_ok}, failed={files_failed})")
	logger.info(f"Rows seen: {total_rows}")
	logger.info(f"Indexed: {total_indexed}")
	logger.info(f"Errors: {total_errors}")
	logger.info(f"Elapsed: {format_duration(total_elapsed_seconds)}")

	if total_elapsed_seconds > 0:
		rows_per_sec = total_rows / total_elapsed_seconds
		indexed_per_sec = total_indexed / total_elapsed_seconds
		logger.info(f"Rows/sec: {rows_per_sec:,.2f}")
		logger.info(f"Indexed/sec: {indexed_per_sec:,.2f}")

	if reports:
		slowest = max(reports, key=lambda item: item.duration_seconds)
		logger.info(
			"Slowest file: "
			f"{slowest.file_path.name} ({format_duration(slowest.duration_seconds)})"
		)

	logger.info("Per-file results:")
	for item in reports:
		base = (
			f"- {item.file_path.name}: status={item.status}, "
			f"rows={item.rows}, indexed={item.indexed}, errors={item.errors}, "
			f"elapsed={format_duration(item.duration_seconds)}, target={item.target_name}"
		)
		if item.message:
			base = f"{base}, message={item.message}"
		logger.info(base)


def main() -> int:
	args = parse_args()
	configure_logging(args.log_level, args.log_file)
	configure_csv_field_size_limit()
	if args.chunk_size <= 0:
		logger.error("--chunk-size must be greater than 0")
		return 1
	if args.max_chunk_bytes <= 0:
		logger.error("--max-chunk-bytes must be greater than 0")
		return 1
	if args.parallel_workers <= 0:
		logger.error("--parallel-workers must be greater than 0")
		return 1
	if args.create_index_shards is not None and args.create_index_shards <= 0:
		logger.error("--create-index-shards must be greater than 0")
		return 1
	if args.create_index_replicas is not None and args.create_index_replicas < 0:
		logger.error("--create-index-replicas must be greater than or equal to 0")
		return 1
	if args.output == "kafka" and args.op_type != "index":
		logger.warning("--op-type is only used for Elasticsearch output and will be ignored")
	if args.output == "kafka" and args.fast_index_mode:
		logger.warning("--fast-index-mode is only used for Elasticsearch output and will be ignored")
	if args.output == "kafka" and (
		args.create_index_shards is not None
		or args.create_index_replicas is not None
		or args.create_index_refresh_interval is not None
	):
		logger.warning(
			"--create-index-* settings are only used for Elasticsearch output and will be ignored"
		)

	run_start = time.perf_counter()
	reports: List[FileReport] = []
	error_log_handle: TextIO | None = None

	input_dir = Path(args.input_dir)
	try:
		files = list_tsv_files(input_dir=input_dir, pattern=args.pattern)
	except Exception as exc:
		logger.error(f"Error locating TSV files: {exc}")
		return 1

	if args.dry_run:
		logger.info("Dry run mode: no data will be indexed")
		for file_path in files:
			file_start = time.perf_counter()
			target_name = (
				args.kafka_topic
				if args.output == "kafka"
				else clean_index_name(f"{args.index_prefix}-{file_path.stem}")
			)
			rows = count_rows(file_path)
			elapsed = time.perf_counter() - file_start
			reports.append(
				FileReport(
					file_path=file_path,
					target_name=target_name or "(unset)",
					rows=rows,
					indexed=0,
					errors=0,
					duration_seconds=elapsed,
					status="dry-run",
				)
			)
			logger.info(f"{file_path}: rows={rows}, target={target_name}")

		print_summary(
			mode="dry-run",
			reports=reports,
			total_elapsed_seconds=time.perf_counter() - run_start,
		)
		return 0

	if args.error_log_file:
		error_log_path = Path(args.error_log_file)
		error_log_path.parent.mkdir(parents=True, exist_ok=True)
		error_log_handle = error_log_path.open("w", encoding="utf-8")
		logger.info(f"Writing failed-row details to: {error_log_path}")

	try:
		client: Elasticsearch | None = None
		producer: Any = None
		if args.output == "elasticsearch":
			try:
				client = create_client(args)
			except Exception as exc:
				logger.error(f"Error connecting to Elasticsearch: {exc}")
				return 1
		else:
			try:
				producer = create_kafka_producer(args)
			except Exception as exc:
				logger.error(f"Error connecting to Kafka: {exc}")
				return 1

		total_success = 0
		total_errors = 0

		for file_path in files:
			file_start = time.perf_counter()
			target_name = (
				clean_index_name(f"{args.index_prefix}-{file_path.stem}")
				if args.output == "elasticsearch"
				else str(args.kafka_topic)
			)
			try:
				if args.output == "elasticsearch":
					if client is None:
						raise RuntimeError("Elasticsearch client not initialized")
					maybe_prepare_index(
						client=client,
						index_name=target_name,
						create_index=args.create_index,
						overwrite_index=args.overwrite_index,
						create_index_shards=args.create_index_shards,
						create_index_replicas=args.create_index_replicas,
						create_index_refresh_interval=args.create_index_refresh_interval,
					)
					original_settings: Dict[str, str] | None = None
					try:
						if args.fast_index_mode:
							original_settings = apply_fast_index_mode(
								client=client,
								index_name=target_name,
							)

						success, errors = bulk_load_file(
							client=client,
							file_path=file_path,
							index_name=target_name,
							id_column=args.id_column,
							chunk_size=args.chunk_size,
							max_chunk_bytes=args.max_chunk_bytes,
							parallel_workers=args.parallel_workers,
							op_type=args.op_type,
							error_log_handle=error_log_handle,
							error_sample_limit=args.error_sample_limit,
						)
					finally:
						if args.fast_index_mode and original_settings is not None:
							try:
								restore_index_settings(
									client=client,
									index_name=target_name,
									original_settings=original_settings,
								)
							except Exception as restore_exc:
								logger.error(
									"Failed to restore index settings for "
									f"{target_name}: {restore_exc}"
								)
				else:
					if producer is None:
						raise RuntimeError("Kafka producer not initialized")
					success, errors = produce_file_to_kafka(
						producer=producer,
						file_path=file_path,
						topic=target_name,
						key_column=args.kafka_key_column or args.id_column,
						chunk_size=args.chunk_size,
					)
				rows = success + errors
				elapsed = time.perf_counter() - file_start
				total_success += success
				total_errors += errors
				reports.append(
					FileReport(
						file_path=file_path,
						target_name=target_name,
						rows=rows,
						indexed=success,
						errors=errors,
						duration_seconds=elapsed,
						status="ok",
					)
				)
				operation = "Loaded" if args.output == "elasticsearch" else "Produced"
				logger.success(f"{operation} {file_path.name} -> {target_name} (indexed={success}, errors={errors})")
			except Exception as exc:
				elapsed = time.perf_counter() - file_start
				logger.error(f"Error processing {file_path}: {exc}")
				total_errors += 1
				reports.append(
					FileReport(
						file_path=file_path,
						target_name=target_name,
						rows=0,
						indexed=0,
						errors=1,
						duration_seconds=elapsed,
						status="failed",
						message=str(exc),
					)
				)

		logger.info(f"Finished. Indexed={total_success}, Errors={total_errors}")
		print_summary(
			mode="index",
			reports=reports,
			total_elapsed_seconds=time.perf_counter() - run_start,
		)
		return 0 if total_errors == 0 else 2
	finally:
		if error_log_handle is not None:
			error_log_handle.close()


if __name__ == "__main__":
	raise SystemExit(main())
