#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v84.

Emergency storage hardening for persistent Railway volume.

The bot historically attached an unbounded FileHandler to /data/aster_bot.log.
On a 0.5 GB volume this eventually filled the filesystem, causing state.json,
SQLite and journals to fail with ENOSPC. Railway already captures stdout, so the
persistent duplicate log is unnecessary for correctness.

v84:
- disables the persistent FileHandler while retaining stdout/Railway logs;
- truncates ONLY /data/aster_bot.log at startup (diagnostic duplicate, never
  state/ledger/trades/order journal);
- fsyncs the truncated file;
- preserves state.json, backup, SQLite ledger, trades, order journal, positions,
  orders, bankrolls, anchors, recovery and all strategy/risk parameters.

No exchange order is sent/cancelled by this maintenance.
"""

import os
import logging
import runner_v83 as base

bot = base.bot


def _storage_recover_v84():
    removed = []
    # Close/remove only handlers pointing at the known duplicate diagnostic log.
    for h in list(bot.logger.handlers):
        if isinstance(h, logging.FileHandler):
            try:
                path = os.path.realpath(getattr(h, "baseFilename", ""))
                target = os.path.realpath(str(bot.LOG_FILE))
                if path == target:
                    bot.logger.removeHandler(h)
                    h.flush(); h.close()
                    removed.append(path)
            except Exception:
                pass
    # Reclaim the unbounded duplicate log without touching durable accounting.
    target = str(bot.LOG_FILE)
    try:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fsync(fd)
        os.close(fd)
    except Exception as exc:
        # stdout remains available even if volume is completely unable to truncate.
        bot.logger.error("STORAGE HARDENING V84 | diagnostic log truncate failed | %s", exc)
    bot.logger.warning(
        "STORAGE HARDENING V84 ACTIVE | persistent_file_logging=DISABLED | duplicate_log_truncated=%s | "
        "state=UNTOUCHED ledger=UNTOUCHED trades=UNTOUCHED journal=UNTOUCHED positions=UNTOUCHED orders=UNTOUCHED",
        target,
    )


_storage_recover_v84()
bot.VERSION = f"{bot.VERSION}-storage-hardening-v84"


def main():
    base.main()


if __name__ == "__main__":
    main()
