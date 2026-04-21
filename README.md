<p align="center">
  <img src="assets/WLD_Logo.png" alt="WorkLog Diary logo" width="360">
</p>

# WorkLogDiary

## What is this?
WorkLogDiary is a Windows app that helps you review how your workday went.
It runs in the background, captures work activity from your PC, and turns it into local summaries using LM Studio.

## What does it do?
- Tracks the window you are using while it is monitoring.
- Saves screenshots and keyboard activity when allowed.
- Pauses automatically when a blocked app is active or your PC is locked.
- Creates local summaries and a daily recap.
- Shows your summaries in a simple calendar view.

## Local Database Encryption

WorkLog Diary stores its local database in an encrypted SQLCipher file.

- The database key is generated randomly and protected with Windows DPAPI using the current user context.
- The `worklog_diary.db` file cannot be opened outside the application without the matching Windows user context.
- The protected key blob is stored separately in `db_key.bin`.
- Temporary artifacts such as screenshots, logs, cache files, and exports are not encrypted.

If you back up the app data, keep the database file and `db_key.bin` together. Losing the Windows user context that protected the key can make the database unreadable, and there is no recovery key.

## Requirements
- LM Studio running locally
- Gemma 4 E2B or E4B model loaded in LM Studio
- Windows 10/11 with the same user account that initialized the encrypted database

## How to use
1. Download the app from GitHub Releases.
2. Run the executable.
3. Make sure LM Studio is running with the correct model.

## Download
Get the latest version from [GitHub Releases](https://github.com/markod0925/WLD/releases).

## Documentation
- [Technical documentation](docs/README_TECHNICAL.md)
- [Database encryption](docs/ENCRYPTION.md)
