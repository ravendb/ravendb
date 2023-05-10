using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands;

public class DelayBackupCommand : UpdateValueForDatabaseCommand
{
    public long TaskId;
    public DateTime DelayUntil;
    public DateTime OriginalBackupTime;

    // ReSharper disable once UnusedMember.Local
    private DelayBackupCommand()
    {
        // for deserialization
    }

    public DelayBackupCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
    }

    public override string GetItemId()
    {
        return PeriodicBackupStatus.GenerateItemName(DatabaseName, TaskId);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(TaskId)] = TaskId;
        json[nameof(DelayUntil)] = DelayUntil;
        json[nameof(OriginalBackupTime)] = OriginalBackupTime;
    }

    protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
    {
        if (existingValue != null)
        {
            existingValue.Modifications = new DynamicJsonValue
            {
                [nameof(DelayUntil)] = DelayUntil,
                [nameof(OriginalBackupTime)] = OriginalBackupTime
            };
            return context.ReadObject(existingValue, GetItemId());
        }

        var status = new PeriodicBackupStatus
        {
            DelayUntil = DelayUntil,
            OriginalBackupTime = OriginalBackupTime
        };
        return context.ReadObject(status.ToJson(), GetItemId());
    }

    public override object GetState()
    {
        return new DelayBackupCommandState
        {
            TaskId = TaskId,
            DelayUntil = DelayUntil,
            OriginalBackupTime = OriginalBackupTime
        };
    }

    public class DelayBackupCommandState
    {
        public long TaskId;
        public DateTime DelayUntil;
        public DateTime OriginalBackupTime;
    }
}
