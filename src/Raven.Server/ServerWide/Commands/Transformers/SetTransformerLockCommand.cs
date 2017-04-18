using Raven.Client.Documents.Transformers;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Transformers
{
    public class SetTransformerLockCommand : UpdateDatabaseCommand
    {
        public string TransformerName;

        public TransformerLockMode LockMode;

        public SetTransformerLockCommand()
            : base(null)
        {
            // for deserialization
        }

        public SetTransformerLockCommand(string name, TransformerLockMode mode, string databaseName)
            : base(databaseName)
        {
            TransformerName = name;
            LockMode = mode;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Transformers[TransformerName].LockMode = LockMode;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TransformerName)] = TransformerName;
            json[nameof(LockMode)] = LockMode;
        }
    }
}