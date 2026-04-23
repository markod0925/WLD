from .crash_monitor import CrashMonitor, append_emergency_marker, run_protected

CrashReporter = CrashMonitor

__all__ = ["CrashMonitor", "CrashReporter", "append_emergency_marker", "run_protected"]
