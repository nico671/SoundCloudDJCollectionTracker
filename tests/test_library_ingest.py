import unittest

from src.library_ingest import AudioMetadata, high_confidence, score_track


class LibraryIngestTests(unittest.TestCase):
    def test_exact_tag_match_is_high_confidence(self):
        metadata = AudioMetadata("Shine", "DJ Example", "", 300.0, 128.0, "Am")
        row = {
            "title": "Shine",
            "artist": "DJ Example",
            "duration_ms": 300000,
            "bpm": 128,
            "key_signature": "Am",
        }
        score = score_track(metadata, row)
        self.assertGreaterEqual(score, 0.94)

    def test_remix_mismatch_lowers_score(self):
        metadata = AudioMetadata("Shine (Original Mix)", "DJ Example", "", None, None, "")
        remix = {"title": "Shine (Remix)", "artist": "DJ Example"}
        self.assertLess(score_track(metadata, remix), 0.94)

    def test_clear_winner_is_required(self):
        first = type("Candidate", (), {"score": 0.96})()
        second = type("Candidate", (), {"score": 0.90})()
        self.assertFalse(high_confidence([first, second]))
