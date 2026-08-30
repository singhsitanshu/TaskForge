import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.run import BenchmarkError, Harness
from benchmarks.trust import create_manifest, sha256_file, verify_manifest
from benchmarks.trusted import (
    TrustedRun,
    create_run_directory,
    image_provenance,
    new_run_id,
    run_contract,
    source_provenance,
)


COMMIT_SHA = "a" * 40
TREE_HASH = "b" * 40


def git_runner(status: str):
    def run(arguments, **_kwargs):
        command = tuple(arguments)
        outputs = {
            ("git", "status", "--porcelain"): status,
            ("git", "branch", "--show-current"): "main\n",
            ("git", "describe", "--always", "--tags", "--dirty"): "v1.0-1-gaaaaaaa\n",
            ("git", "rev-parse", "HEAD"): COMMIT_SHA + "\n",
            ("git", "rev-parse", "HEAD^{tree}"): TREE_HASH + "\n",
        }
        return subprocess.CompletedProcess(arguments, 0, outputs[command], "")

    return run


class PublishableContractTests(unittest.TestCase):
    def test_clean_tree_passes_and_captures_git_identity(self) -> None:
        with mock.patch("benchmarks.trusted.run_command", side_effect=git_runner("")):
            source = source_provenance(require_clean=True)
        self.assertTrue(source["clean"])
        self.assertEqual(source["git_commit_sha"], COMMIT_SHA)
        self.assertEqual(source["git_tree_hash"], TREE_HASH)
        self.assertEqual(source["git_branch"], "main")
        self.assertEqual(source["git_describe"], "v1.0-1-gaaaaaaa")

    def test_dirty_tree_fails_publishable_contract(self) -> None:
        dirty = " M benchmarks/trusted.py\n"
        with mock.patch(
            "benchmarks.trusted.run_command", side_effect=git_runner(dirty)
        ):
            with self.assertRaisesRegex(
                BenchmarkError, "requires a clean working tree"
            ):
                run_contract(development=False)

    def test_development_contract_allows_dirty_tree_and_marks_unpublishable(
        self,
    ) -> None:
        dirty = " M benchmarks/trusted.py\n"
        with mock.patch(
            "benchmarks.trusted.run_command", side_effect=git_runner(dirty)
        ):
            contract = run_contract(development=True)
        self.assertFalse(contract["publishable"])
        self.assertEqual(contract["publication_status"], "UNPUBLISHABLE")
        self.assertFalse(contract["source"]["clean"])

    def test_run_ids_are_unique(self) -> None:
        source = {"git_commit_sha": COMMIT_SHA}
        self.assertNotEqual(
            new_run_id(source, "release"), new_run_id(source, "release")
        )


class ImageIdentityTests(unittest.TestCase):
    def test_all_required_image_identities_are_captured(self) -> None:
        class FakeHarness:
            project = "taskforge-tf012-provenance-test"
            image = "taskforge-tf012-provenance-test-loadgen"

        def inspect(arguments, **_kwargs):
            image_name = arguments[3]
            identity = image_name.replace("taskforge-tf012-provenance-test-", "")
            payload = {
                "Id": f"sha256:{identity}",
                "RepoDigests": [f"{image_name}@sha256:{identity}-digest"],
                "RepoTags": [f"{image_name}:latest"],
                "Created": "2026-01-01T00:00:00Z",
                "Architecture": "arm64",
                "Os": "linux",
            }
            return subprocess.CompletedProcess(arguments, 0, json.dumps(payload), "")

        with mock.patch("benchmarks.trusted.run_command", side_effect=inspect):
            images = image_provenance(FakeHarness())
        self.assertEqual(set(images), {"api", "worker", "scheduler", "load_generator"})
        for identity in images.values():
            self.assertTrue(identity["image_id"].startswith("sha256:"))
            self.assertEqual(len(identity["repo_digests"]), 1)


class EnvironmentIdentityTests(unittest.TestCase):
    def test_required_environment_identity_is_captured(self) -> None:
        harness = object.__new__(Harness)
        harness.env = {}
        harness.project = "taskforge-tf012-provenance-test"
        harness.profile = {"name": "test"}
        harness.psql = lambda _query: '{"version":"PostgreSQL 16.1"}'

        def command(arguments, **_kwargs):
            output = {
                ("git", "rev-parse", "HEAD"): COMMIT_SHA,
                ("git", "status", "--short"): "",
                (
                    "docker",
                    "version",
                    "--format",
                    "{{json .}}",
                ): '{"Server":{"Version":"27"}}',
                (
                    "docker",
                    "info",
                    "--format",
                    "{{json .}}",
                ): '{"Architecture":"arm64"}',
                (
                    "docker",
                    "run",
                    "--rm",
                    "golang:1.23-alpine",
                    "go",
                    "version",
                ): "go version go1.23 linux/arm64",
            }[tuple(arguments)]
            return subprocess.CompletedProcess(arguments, 0, output, "")

        with (
            mock.patch("benchmarks.run.run_command", side_effect=command),
            mock.patch("benchmarks.run.host_cpu_description", return_value="Test CPU"),
            mock.patch("benchmarks.run.host_memory_bytes", return_value=16_000_000_000),
            mock.patch("benchmarks.run.os.cpu_count", return_value=8),
            mock.patch("benchmarks.run.platform.platform", return_value="TestOS-arm64"),
            mock.patch("benchmarks.run.platform.system", return_value="TestOS"),
            mock.patch("benchmarks.run.platform.release", return_value="1.0"),
            mock.patch("benchmarks.run.platform.python_version", return_value="3.13.1"),
        ):
            environment = harness.environment()

        self.assertEqual(environment["host_cpu"], "Test CPU")
        self.assertEqual(environment["host_logical_cpus"], 8)
        self.assertEqual(environment["host_memory_bytes"], 16_000_000_000)
        self.assertEqual(environment["platform"], "TestOS-arm64")
        self.assertEqual(environment["docker_version"], '{"Server":{"Version":"27"}}')
        self.assertEqual(environment["postgresql"], '{"version":"PostgreSQL 16.1"}')
        self.assertEqual(environment["go_version"], "go version go1.23 linux/arm64")
        self.assertEqual(environment["python_version"], "3.13.1")


class ArtifactIdentityTests(unittest.TestCase):
    def test_manifest_hashes_validate_and_tampering_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "metadata.json").write_text('{"publishable": true}\n')
            (directory / "evidence.txt").write_text("immutable evidence\n")
            manifest = create_manifest(directory)

            by_path = {item["path"]: item for item in manifest["artifacts"]}
            self.assertEqual(set(by_path), {"metadata.json", "evidence.txt"})
            self.assertEqual(
                by_path["metadata.json"]["sha256"],
                sha256_file(directory / "metadata.json"),
            )
            self.assertEqual(verify_manifest(directory), (True, []))

            (directory / "evidence.txt").write_text("tampered\n")
            valid, errors = verify_manifest(directory)
            self.assertFalse(valid)
            self.assertIn("evidence.txt hash mismatch", errors)

    def test_unlisted_artifact_fails_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "metadata.json").write_text("{}\n")
            create_manifest(directory)
            (directory / "late-artifact.txt").write_text("not in manifest\n")
            valid, errors = verify_manifest(directory)
            self.assertFalse(valid)
            self.assertIn("late-artifact.txt is not listed", errors)

    def test_every_saved_trial_contains_metadata_and_manifest(self) -> None:
        class FakeHarness:
            @staticmethod
            def trial_configuration():
                return {"poll_interval": "test"}

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            trusted = TrustedRun(
                FakeHarness(),
                output,
                {},
                "run-id",
                {"publication_status": "UNPUBLISHABLE"},
            )
            result = trusted.save_trial(
                scenario="provenance",
                variant="identity",
                block=1,
                trial=1,
                workers=1,
                schedulers=1,
                count=0,
                queue="test",
                task_type="test.noop",
                payload={},
                submission={},
                tasks=[],
                attempts=[],
                raw={"missing_queue_evidence": 0, "negative_durations": {}},
                correctness_result={"passed": True},
                prom_start={},
                prom_end={},
                reconciliation={"status": "PASS"},
                resource_samples=[],
                metadata={},
            )
            directory = output / result["artifacts"]["directory"]
            self.assertTrue((directory / "metadata.json").is_file())
            self.assertTrue((directory / "manifest.json").is_file())
            metadata = json.loads((directory / "metadata.json").read_text())
            self.assertEqual(
                metadata["provenance"]["publication_status"], "UNPUBLISHABLE"
            )
            self.assertEqual(verify_manifest(directory), (True, []))

    def test_existing_run_directory_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "existing-run"
            directory.mkdir()
            marker = directory / "marker.txt"
            marker.write_text("preserve me\n")
            with self.assertRaisesRegex(BenchmarkError, "refusing to overwrite"):
                create_run_directory(directory)
            self.assertEqual(marker.read_text(), "preserve me\n")


if __name__ == "__main__":
    unittest.main()
