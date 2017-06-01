using System.Diagnostics;
using Raven.Client.Server;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteDatabaseWatcherCommand : UpdateDatabaseCommand
    {
        public long TaskId;

        public DeleteDatabaseWatcherCommand() : base(null)
        {

        }

        public DeleteDatabaseWatcherCommand(long taskId, string databaseName) : base(databaseName)
        {
            TaskId = taskId;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Debug.Assert(TaskId != 0);
            record.Topology.RemoveWatcher(TaskId);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TypeConverter.ToBlittableSupportedType(TaskId);
        }
    }
}
