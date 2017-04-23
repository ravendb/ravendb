using Raven.Client.Documents;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Transformers
{
    public class DeleteTransformerCommand : UpdateDatabaseCommand
    {
        public string TransformerName;

        public DeleteTransformerCommand()
            : base(null)
        {
            // for deserialization
        }

        public DeleteTransformerCommand(string name, string databaseName)
            : base(databaseName)
        {
            TransformerName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteTransformer(TransformerName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TransformerName)] = TransformerName;
        }
    }
}