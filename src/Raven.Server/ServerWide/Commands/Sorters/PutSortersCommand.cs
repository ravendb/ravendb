using System.Collections.Generic;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sorters
{
    public class PutSortersCommand : UpdateDatabaseCommand
    {
        public List<SorterDefinition> Sorters = new List<SorterDefinition>();

        public PutSortersCommand() : base(null)
        {
            // for deserialization
        }

        public PutSortersCommand(string databaseName)
            : base(databaseName)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Sorters != null)
            {
                foreach (var definition in Sorters)
                    record.AddSorter(definition);
            }

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Sorters)] = TypeConverter.ToBlittableSupportedType(Sorters);
        }
    }
}
