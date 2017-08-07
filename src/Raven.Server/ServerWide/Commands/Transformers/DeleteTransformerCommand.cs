using Raven.Client.ServerWide;
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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteTransformer(TransformerName);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TransformerName)] = TransformerName;
        }
    }
}