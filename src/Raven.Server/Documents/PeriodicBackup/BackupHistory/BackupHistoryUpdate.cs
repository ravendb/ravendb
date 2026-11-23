using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed record BackupHistoryUpdate(string DatabaseName, long TaskId, BackupHistoryEntry Entry, BackupResult Result);
