using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationCommand : UpdateDatabaseCommand
    {
        public ExternalReplication Watcher;

        public UpdateExternalReplicationCommand() : base(null)
        {

        }

        public UpdateExternalReplicationCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Watcher == null)
                return null;

            //ExternalReplication.EnsureUniqueDbAndUrl(record.ExternalReplication,Watcher);

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = etag;
            }
            else
            {
                //modified watcher, remove the old one
                ExternalReplication.RemoveWatcher(ref record.ExternalReplication, Watcher.TaskId);
            }

            record.ExternalReplication.Add(Watcher);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
