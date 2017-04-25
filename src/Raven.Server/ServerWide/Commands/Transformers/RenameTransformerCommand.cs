using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Transformers;
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
            if (record.Transformers.ContainsKey(NewTransformerName))
            {
                throw new InvalidOperationException($"Could not rename transformer {TransformerName} because there already is a transformer named {TransformerName}");
            }
            if (record.Indexes.ContainsKey(NewTransformerName) || record.AutoIndexes.ContainsKey(NewTransformerName))
            {
                throw new InvalidOperationException($"Could not rename transformer {TransformerName} because there already is an index named {TransformerName}");
            }
            if (record.Transformers.TryGetValue(TransformerName, out TransformerDefinition transformer))
            {
                transformer.Etag = etag;
                transformer.Name = NewTransformerName;

                record.AddTransformer(transformer);
                record.Transformers.Remove(TransformerName);
                return;
            }

            throw new InvalidOperationException($"Could not rename transformer {TransformerName} because it was not found in DatabaseRecord");
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TransformerName)] = TransformerName;
            json[nameof(NewTransformerName)] = NewTransformerName;
        }
    }
}