import threading
import time

from spikemon.follow import follow


def test_follow_prints_tail_then_new_alerts_live(tmp_path):
    log = tmp_path / "alerts.log"
    log.write_text("[t] alert #1\nA one\n\n[t] alert #2\nA two\n\n[t] alert #3\nA three\n\n", encoding="utf-8")
    seen, stop = [], threading.Event()
    th = threading.Thread(target=follow, args=(str(log), 2, seen.append, stop.is_set, 0.05), daemon=True)
    th.start()
    time.sleep(0.3)
    assert [b.splitlines()[0] for b in seen[1:]] == ["[t] alert #2", "[t] alert #3"]   # tail of 2, no #1
    with open(log, "a", encoding="utf-8") as fh:
        fh.write("[t] alert #4\nfresh ")        # half written: must not be printed yet
        fh.flush()
        time.sleep(0.2)
        assert not any("alert #4" in s for s in seen)
        fh.write("one ⚡\n\n")
        fh.flush()
    time.sleep(0.3)
    stop.set()
    th.join(2)
    assert any("alert #4" in s and "fresh one" in s for s in seen)


def test_follow_waits_for_file_to_appear(tmp_path):
    log = tmp_path / "later.log"
    seen, stop = [], threading.Event()
    th = threading.Thread(target=follow, args=(str(log), 3, seen.append, stop.is_set, 0.05), daemon=True)
    th.start()
    time.sleep(0.2)
    log.write_text("[t] alert #1\nhello\n\n", encoding="utf-8")
    time.sleep(0.3)
    stop.set()
    th.join(2)
    assert any("Waiting for the first alert" in s for s in seen)
