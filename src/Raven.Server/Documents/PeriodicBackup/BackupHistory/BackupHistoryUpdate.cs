using System;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed record BackupHistoryUpdate(string DatabaseName, PeriodicBackupStatus Status, BackupResult Result, DateTime CutoffTime);
