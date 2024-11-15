using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes;
using Raven.Server.Extensions;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public sealed class PutIndexCommand : UpdateDatabaseCommand
    {
        public IndexDefinition Definition;

        public IndexDeploymentMode? DefaultDeploymentMode;

        public PutIndexCommand()
        {
            // for deserialization
        }

        public PutIndexCommand(IndexDefinition definition, string databaseName, string source, DateTime createdAt, string uniqueRequestId, int revisionsToKeep, IndexDeploymentMode deploymentMode)
            : base(databaseName, uniqueRequestId)
        {
            Definition = definition;
            Definition.ClusterState ??= new IndexDefinitionClusterState();
            Source = source;
            CreatedAt = createdAt;
            RevisionsToKeep = revisionsToKeep;
            DefaultDeploymentMode = deploymentMode;
        }

        public DateTime CreatedAt { get; set; }

        public string Source { get; set; }
        
        public int RevisionsToKeep { get; set; }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var indexValidator = new StaticIndexNameValidator(record);
            indexValidator.Validate(Definition);

            try
            {
                record.AddIndex(Definition, Source, CreatedAt, etag, RevisionsToKeep, DefaultDeploymentMode ?? IndexDeploymentMode.Parallel);

            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to update index", e);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
            json[nameof(Source)] = Source;
            json[nameof(CreatedAt)] = CreatedAt;
            json[nameof(RevisionsToKeep)] = RevisionsToKeep;
            json[nameof(DefaultDeploymentMode)] = DefaultDeploymentMode;
        }

        public override string AdditionalDebugInformation(Exception exception)
        {
            var msg = $"Index name: '{Definition.Name}' for database '{DatabaseName}'";
            if (exception != null)
            {
                msg += $" Exception: {exception}.";
            }

            return msg;
        }

        public class StaticIndexNameValidator
        {
            private readonly DatabaseRecord _record;
            private HashSet<string> _indexNames;
            private HashSet<string> _safeFileSystemIndexNames;

            public StaticIndexNameValidator(DatabaseRecord record)
            {
                _record = record;
            }

            public void Validate(IndexDefinition definition)
            {
                if (_record.Indexes.TryGetValue(definition.Name, out _))
                    return;

                // this is not an update to an existing index. we'll check for:
                // - directory name collisions
                // - index name case sensitivity

                _indexNames ??= _record.Indexes.Select(x => x.Value.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
                _safeFileSystemIndexNames ??= _record.Indexes.Select(x => IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(x.Value.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

                if (_indexNames.Add(definition.Name) == false)
                {
                    throw new IndexCreationException($"Can not add index: {definition.Name} because an index with the same name but different casing already exist");
                }

                var safeFileSystemIndexName = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(definition.Name);
                if (_safeFileSystemIndexNames.Add(safeFileSystemIndexName) == false)
                {
                    var existingIndexName = _indexNames.FirstOrDefault(x =>
                        x.Equals(definition.Name, StringComparison.OrdinalIgnoreCase) == false &&
                        safeFileSystemIndexName.Equals(IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(x), StringComparison.OrdinalIgnoreCase));

                    throw new IndexCreationException(
                        $"Could not create index '{definition.Name}' because it would result in directory name collision with '{existingIndexName}' index");
                }
            }
        }
    }
}
