using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Transformers
{
    public class RenameTransformerCommand : UpdateDatabaseCommand
    {
        public string TransformerName;

        public string NewTransformerName;

        public RenameTransformerCommand()
            : base(null)
        {
            // for deserialization
        }

        public RenameTransformerCommand(string name, string newName, string databaseName)
            : base(databaseName)
        {
            TransformerName = name;
            NewTransformerName = newName;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var transformer = record.Transformers[TransformerName];
            transformer.Etag = etag;
            transformer.Name = NewTransformerName;

            record.AddTransformer(transformer);
            record.Transformers.Remove(TransformerName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TransformerName)] = TransformerName;
            json[nameof(NewTransformerName)] = NewTransformerName;
        }
    }
}