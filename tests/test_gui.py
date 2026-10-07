"""Desktop regression checks without public API requests."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
try:
    import tkinter as tk

    from mgnrega_assets.gui import App
except ImportError:
    tk = None
from mgnrega_assets.scraper import Config
from mgnrega_assets.states import STATES


class GuiTests(unittest.TestCase):
    def setUp(self):
        if tk is None:
            self.skipTest("Tkinter is not installed")
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"Tk display unavailable: {exc}")
        self.root.withdraw()
        with patch.object(App, "refresh"):
            self.app = App(self.root)

    def tearDown(self):
        if hasattr(self, "root"):
            for callback in self.root.tk.call("after", "info"):
                self.root.after_cancel(callback)
            self.root.destroy()

    def test_no_default_state(self):
        self.assertEqual(self.app.code("state"), "")
        with self.assertRaises(ValueError):
            self.app.config()
        self.assertEqual(set(self.app.maps["state"].values()), {"", *STATES})

    def test_queue_snapshots_and_remove(self):
        self.app.set_options("state", STATES, "32")
        self.app.add()
        self.app.set_options("state", STATES, "16")
        self.app.add()
        self.assertEqual([c.state for c in self.app.jobs], ["32", "16"])
        self.app.joblist.selection_set(0)
        self.app.remove()
        self.assertEqual([c.state for c in self.app.jobs], ["16"])

    def test_changing_parent_clears_children(self):
        self.app.set_options("state", STATES, "16")
        self.app.set_options("district", {"1234": "Old district"}, "1234")
        self.app.set_options("block", {"1234001": "Old block"}, "1234001")
        with patch.object(self.app, "lookup") as lookup:
            self.app.changed("state")
            self.assertEqual(lookup.call_args.args[2]["state_code"], "16")
        self.assertEqual(self.app.code("district"), "All")
        self.assertEqual(self.app.code("block"), "All")

    def test_stale_response_is_discarded(self):
        self.app.versions["district"] = 2
        self.app.lookups = 1
        self.app.events.put(("options", "district", 1, {"1234": "Stale district"}))
        self.app.poll()
        self.assertNotIn("1234", self.app.maps["district"].values())
        self.assertEqual(self.app.lookups, 0)

    def test_load_discards_old_location_choices(self):
        self.app.set_options("district", {"1234": "Old district"}, "1234")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "settings.json"
            path.write_text(json.dumps({"state": "16", "district": "1601"}))
            with patch(
                "mgnrega_assets.gui.filedialog.askopenfilename", return_value=str(path)
            ):
                self.app.load()
        self.assertEqual(self.app.code("state"), "16")
        self.assertEqual(self.app.code("district"), "1601")
        self.assertNotIn("1234", self.app.maps["district"].values())

    def test_failed_job_does_not_stop_queue(self):
        self.app.jobs = [Config(state="32"), Config(state="16")]
        with (
            patch("mgnrega_assets.gui.threading.Thread") as thread,
            patch(
                "mgnrega_assets.gui.run",
                side_effect=[ValueError("failed lookup"), {"status": "complete"}],
            ) as run,
        ):
            thread.return_value.start.side_effect = lambda: thread.call_args.kwargs[
                "target"
            ]()
            self.app.start_run()
            self.app.poll()
            self.assertEqual(run.call_count, 2)
            self.assertFalse(self.app.running)
            self.assertIn("incomplete", self.app.status.get())


if __name__ == "__main__":
    unittest.main()
