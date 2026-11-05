using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class EditLockModeCommand : UpdateDatabaseCommand
    {
        public override DatabaseRecordSections GetChangedSections() => DatabaseRecordSections.Configuration;

        public DatabaseLockMode LockMode;

        public EditLockModeCommand()
        {
            // for deserialization
        }

        public EditLockModeCommand(string databaseName, DatabaseLockMode lockMode, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            LockMode = lockMode;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.LockMode = LockMode;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(LockMode)] = LockMode;
        }
    }
}
