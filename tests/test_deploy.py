import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DeployTest(unittest.TestCase):
    def deploy(self, *args, fail=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "commands.jsonl"
            stub = root / "stub"
            stub.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''\
                import json, os, subprocess, sys
                from pathlib import Path

                program = Path(sys.argv[0]).name
                body = sys.stdin.read() if program == "ssh" else ""
                command = " ".join(sys.argv[2:] if program == "ssh" else sys.argv[1:])
                with open(os.environ["DEPLOY_TEST_LOG"], "a") as output:
                    output.write(json.dumps({"program": program, "command": command, "body": body}) + "\\n")
                failure = os.environ.get("DEPLOY_TEST_FAIL")
                if failure and failure in command + body:
                    raise SystemExit(42)
                if program == "systemctl":
                    if "list-unit-files" in sys.argv:
                        print("market-ingest@binance_um.service enabled enabled")
                    elif "list-units" in sys.argv:
                        print("market-ingest@bybit_linear.service loaded active running collector")
                elif program == "ssh":
                    if sys.argv[-2:] == ["bash", "-s"]:
                        raise SystemExit(subprocess.run(["bash", "-s"], input=body, text=True).returncode)
                    if "rev-parse --short=12 HEAD" in command:
                        print("123456789abc")
                '''))
            stub.chmod(0o755)
            for name in ("ssh", "rsync", "systemctl"):
                (root / name).symlink_to(stub)
            env = {**os.environ, "PATH": f"{root}:{os.environ['PATH']}",
                   "DEPLOY_TEST_LOG": str(log), "DEPLOY_TEST_FAIL": fail,
                   "VPS_ALIAS": "deployment-test-host"}
            result = subprocess.run(["bash", "deploy/deploy.sh", *args], cwd=ROOT, env=env,
                                    stdin=subprocess.DEVNULL, text=True, capture_output=True,
                                    timeout=20)
            commands = [json.loads(line) for line in log.read_text().splitlines()]
            return result, commands

    def stage(self, commands, text):
        return next(i for i, entry in enumerate(commands)
                    if entry["program"] == "ssh" and text in entry["command"])

    def test_prepare_precedes_restart_and_finish_follows_readiness(self):
        result, commands = self.deploy("--git")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        preparation = self.stage(commands, "--phase prepare")
        restart = self.stage(commands, "systemctl restart")
        readiness = [i for i, entry in enumerate(commands)
                     if entry["program"] == "ssh" and "systemctl is-active" in entry["command"]]
        finish = self.stage(commands, "--phase finish")
        self.assertLess(preparation, restart)
        self.assertEqual(len(readiness), 2)
        for ready in readiness:
            self.assertLess(restart, ready)
            self.assertLess(ready, finish)
        self.assertIn("market-ingest@binance_um.service", commands[restart]["command"])
        self.assertIn("market-ingest@bybit_linear.service", commands[restart]["command"])

    def test_finish_can_be_deferred(self):
        result, commands = self.deploy("--git", "--defer-migration-finish")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.stage(commands, "--phase prepare")
        self.stage(commands, "systemctl restart")
        self.assertFalse(any("--phase finish" in entry["command"] for entry in commands))
        self.assertIn("--phase finish --client clickhouse-client", result.stdout)

    def test_failed_preparation_prevents_restart(self):
        result, commands = self.deploy("--git", fail="--phase prepare")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any("systemctl restart" in entry["command"] for entry in commands))
        self.assertFalse(any("--phase finish" in entry["command"] for entry in commands))

    def test_failed_restart_prevents_finish(self):
        result, commands = self.deploy("--git", fail="systemctl restart")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any("--phase finish" in entry["command"] for entry in commands))

    def test_failed_readiness_prevents_finish(self):
        result, commands = self.deploy("--git", fail="systemctl is-active")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any("--phase finish" in entry["command"] for entry in commands))

    def test_failed_discovery_prevents_finish(self):
        result, commands = self.deploy("--git", fail="list-unit-files")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any("systemctl restart" in entry["command"] for entry in commands))
        self.assertFalse(any("--phase finish" in entry["command"] for entry in commands))

    def test_rsync_ships_migrations_and_config_example(self):
        result, commands = self.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        transfer = next(entry["command"] for entry in commands if entry["program"] == "rsync")
        self.assertIn(str(ROOT / "deploy"), transfer)
        self.assertIn(str(ROOT / "config.example.toml"), transfer)
        self.stage(commands, "--phase finish")


if __name__ == "__main__":
    unittest.main()
