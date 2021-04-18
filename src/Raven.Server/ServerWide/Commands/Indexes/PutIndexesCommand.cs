using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class PutIndexesCommand : UpdateDatabaseCommand
    {
        public List<IndexDefinition> Static = new List<IndexDefinition>();

        public List<AutoIndexDefinition> Auto = new List<AutoIndexDefinition>();

        public DateTime CreatedAt { get; set; }

        public string Source { get; set; }

        public int RevisionsToKeep { get; set; }

        public PutIndexesCommand()
        {
            // for deserialization
        }

        public PutIndexesCommand(string databaseName, string source, DateTime createdAt, string uniqueRequestId, int revisionsToKeep)
            : base(databaseName, uniqueRequestId)
        {
            Source = source;
            CreatedAt = createdAt;
            RevisionsToKeep = revisionsToKeep;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var setting = PutIndexCommand.GetGlobalRollingSetting(record);

            if (Static != null)
            {
                foreach (var definition in Static)
                    record.AddIndex(definition, Source, CreatedAt, etag, RevisionsToKeep, setting);
            }

            if (Auto != null)
            {
                foreach (var definition in Auto)
                    record.AddIndex(definition, CreatedAt, etag, setting);
            }

        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Static)] = TypeConverter.ToBlittableSupportedType(Static);
            json[nameof(Auto)] = TypeConverter.ToBlittableSupportedType(Auto);
            json[nameof(Source)] = Source;
            json[nameof(CreatedAt)] = CreatedAt;
            json[nameof(RevisionsToKeep)] = RevisionsToKeep;
        }
    }
}
