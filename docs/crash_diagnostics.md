# Crash Diagnostics (Windows + Python)

WorkLog Diary uses layered crash observability so "silent exits" leave evidence even when one mechanism misses details.

## Diagnostic layers

1. **Python exception hooks** (`sys.excepthook`, `threading.excepthook`, `sys.unraisablehook`)
   - Captures uncaught exceptions on main thread, worker threads, and unraisable contexts.
   - Writes critical log entries with traceback details.
   - Writes a last-gasp marker to `crash_last_gasp.log`.

2. **`faulthandler` trace dumping**
   - Enabled at startup with `all_threads=True`.
   - Appends native-fault-oriented Python tracebacks into `crash_faulthandler.log`.
   - Useful for segfault-like process failures where normal logging may be incomplete.

3. **Persistent session state + heartbeat** (`session_state.json`)
   - Stores run metadata (`session_id`, PID, start time, heartbeat, clean-shutdown flag, app version).
   - If the app did not previously finalize cleanly, next startup logs a prominent
     "previous run ended unexpectedly" event with forensic context.

4. **Windows Error Reporting (WER) LocalDumps**
   - External Windows facility for user-mode crash dumps (`.dmp`) from hard/native crashes.
   - Not managed by the app; configured manually in registry.

## Files to collect after a crash

From the app data/log directories, collect:
- `logs/app.log` (and rotated app logs)
- `logs/crash_faulthandler.log`
- `logs/crash_last_gasp.log`
- `session_state.json`
- Any WER `.dmp` files (if LocalDumps is enabled)

## Enable WER LocalDumps for packaged WorkLog Diary EXE

> Requires administrator rights for machine-wide `HKLM` settings.

For per-executable dump collection (recommended), create this key:

`HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\WLD.exe`

Set values:
- `DumpFolder` (`REG_EXPAND_SZ`) = `C:\ProgramData\WorkLogDiary\Dumps`
- `DumpType` (`REG_DWORD`) = `2` (full dump; `1` = mini dump)
- `DumpCount` (`REG_DWORD`) = `10` (or suitable retention count)

Optional global fallback key:

`HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps`

If both global and exe-specific keys exist, exe-specific settings apply.

### PowerShell example

```powershell
$base = 'HKLM:\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\WLD.exe'
New-Item -Path $base -Force | Out-Null
New-ItemProperty -Path $base -Name DumpFolder -PropertyType ExpandString -Value 'C:\ProgramData\WorkLogDiary\Dumps' -Force | Out-Null
New-ItemProperty -Path $base -Name DumpType -PropertyType DWord -Value 2 -Force | Out-Null
New-ItemProperty -Path $base -Name DumpCount -PropertyType DWord -Value 10 -Force | Out-Null
```

## Verify dump generation

1. Ensure dump folder exists and is writable by the crashing user/process.
2. Reproduce a crash.
3. Confirm `.dmp` file appears in `DumpFolder`.
4. Check Event Viewer:
   - **Windows Logs → Application**
   - Typical sources include **Application Error** and **Windows Error Reporting**.

> Important: the `LocalDumps\<image-name>.exe` key must match the actual crashing process image name exactly.  
> For the packaged PyInstaller release bundle in this repo, that image name is `WLD.exe`.

## Troubleshooting checklist

- Is `session_state.json` updating heartbeat while app runs?
- After abrupt kill, does next startup log `event=previous_run_unexpected_exit`?
- Is `crash_faulthandler.log` present and writable?
- If no Python logs for hard crash, is WER LocalDumps correctly configured for the exact executable name?
- Is dump folder path valid and writable?
