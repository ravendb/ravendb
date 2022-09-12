using System;
using Raven.Client.Documents.Operations.OngoingTasks;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup;

internal class PeriodicBackupInfo
{
    public string Database { get; set; }

    public string Name { get; set; }

    public long TaskId { get; set; }

    public string FullBackupFrequency { get; set; }

    public string IncrementalBackupFrequency { get; set; }

    public NextBackup NextBackup { get; set; }

    public DateTime? CreatedAt { get; set; }

    public bool Disposed { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Database)] = Database,
            [nameof(Name)] = Name,
            [nameof(TaskId)] = TaskId,
            [nameof(FullBackupFrequency)] = FullBackupFrequency,
            [nameof(IncrementalBackupFrequency)] = IncrementalBackupFrequency,
            [nameof(NextBackup)] = NextBackup?.ToJson(),
            [nameof(CreatedAt)] = CreatedAt?.GetDefaultRavenFormat(),
            [nameof(Disposed)] = Disposed
        };
    }
}
