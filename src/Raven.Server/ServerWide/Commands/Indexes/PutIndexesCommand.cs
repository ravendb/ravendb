using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public sealed class PutIndexesCommand : UpdateDatabaseCommand
    {
        public List<IndexDefinition> Static = new List<IndexDefinition>();

        public List<AutoIndexDefinition> Auto = new List<AutoIndexDefinition>();

        public IndexDeploymentMode? DefaultStaticDeploymentMode;

        public IndexDeploymentMode? DefaultAutoDeploymentMode;

        public DateTime CreatedAt { get; set; }

        public string Source { get; set; }
        
        public int RevisionsToKeep { get; set; }

        public PutIndexesCommand()
        {
            // for deserialization
        }

        public PutIndexesCommand(string databaseName, string source, DateTime createdAt, string uniqueRequestId, int revisionsToKeep, IndexDeploymentMode autoDeploymentMode, IndexDeploymentMode staticDeploymentMode)
            : base(databaseName, uniqueRequestId)
        {
            Source = source;
            CreatedAt = createdAt;
            RevisionsToKeep = revisionsToKeep;
            DefaultAutoDeploymentMode = autoDeploymentMode;
            DefaultStaticDeploymentMode = staticDeploymentMode;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Static != null)
            {
                var indexValidator = new PutIndexCommand.StaticIndexNameValidator(record);

                foreach (var definition in Static)
                {
                    indexValidator.Validate(definition);

                    try
                    {
                        record.AddIndex(definition, Source, CreatedAt, etag, RevisionsToKeep, DefaultStaticDeploymentMode ?? IndexDeploymentMode.Parallel);
                    }
                    catch (Exception e)
                    {
                        throw new RachisApplyException($"Failed to update index '{definition.Name}'", e);
                    }
                }
            }

            if (Auto != null)
            {
                foreach (var definition in Auto)
                {
                    try
                    {
                        record.AddIndex(definition, CreatedAt, etag, DefaultAutoDeploymentMode ?? IndexDeploymentMode.Parallel);
                    }
                    catch (Exception e)
                    {
                        throw new RachisApplyException($"Failed to update index '{definition.Name}'", e);
                    }
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Static)] = TypeConverter.ToBlittableSupportedType(Static);
            json[nameof(Auto)] = TypeConverter.ToBlittableSupportedType(Auto);
            json[nameof(Source)] = Source;
            json[nameof(CreatedAt)] = CreatedAt;
            json[nameof(RevisionsToKeep)] = RevisionsToKeep;
            json[nameof(DefaultStaticDeploymentMode)] = DefaultStaticDeploymentMode;
            json[nameof(DefaultAutoDeploymentMode)] = DefaultAutoDeploymentMode;
        }
    }
}
