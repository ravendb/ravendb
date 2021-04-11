using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.Rachis;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class PutIndexCommand : UpdateDatabaseCommand
    {
        public IndexDefinition Definition;

        public PutIndexCommand()
        {
            // for deserialization
        }

        public PutIndexCommand(IndexDefinition definition, string databaseName, string source, DateTime createdAt, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Definition = definition;
            Definition._clusterIndex ??= new ClusterIndex();
            Source = source;
            CreatedAt = createdAt;
        }

        public DateTime CreatedAt { get; set; }

        public string Source { get; set; }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            try
            {
                var indexNames = record.Indexes.Select(x => x.Value.Name).ToHashSet(OrdinalIgnoreCaseStringStructComparer.Instance);

                if (indexNames.Add(Definition.Name) == false && record.Indexes.TryGetValue(Definition.Name, out var definition) == false)
                {
                    throw new InvalidOperationException($"Can not add index: {Definition.Name} because an index with the same name but different casing already exist");
                }
                record.AddIndex(Definition, Source, CreatedAt, etag);
            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to update index", e);
            }
            
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
            json[nameof(Source)] = Source;
            json[nameof(CreatedAt)] = CreatedAt;
        }
    }
}
