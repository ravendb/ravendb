using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public sealed class RenameIndexCommand : UpdateDatabaseCommand
    {
        public string IndexName { get; set; }

        public string NewIndexName { get; set; }

        public DateTime CreatedAt { get; set; }

        public int RevisionsToKeep { get; set; }

        public RenameIndexCommand()
        {
            // for deserialization
        }

        public RenameIndexCommand(string databaseName, string indexName, string newIndexName, DateTime createdAt, int revisionsToKeep, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            IndexName = indexName;
            NewIndexName = newIndexName;
            CreatedAt = createdAt;
            RevisionsToKeep = revisionsToKeep;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var definition = Validate(record, IndexName, NewIndexName);
            var oldName = definition.Name;

            // The definition object moves as it is: AddIndex would give a new ReduceOutputIndex.
            record.Indexes.Remove(oldName);
            definition.Name = NewIndexName;
            definition.ClusterState ??= new IndexDefinitionClusterState();
            definition.ClusterState.LastIndex = etag;
            definition.ClusterState.LastRollingDeploymentIndex = etag;
            definition.ClusterState.RenamedFrom = oldName;
            record.Indexes[NewIndexName] = definition;

            if (record.IndexesHistory != null && record.IndexesHistory.Remove(oldName, out var history))
                record.IndexesHistory[NewIndexName] = history;

            record.AddIndexHistory(definition, $"Renamed from '{oldName}'", RevisionsToKeep, CreatedAt);

            var rollingIndex = record.InitializeRollingDeployment(NewIndexName, CreatedAt, etag);
            rollingIndex.RenamedFrom = oldName;
        }

        // The endpoint also calls this, to fail fast with the same rules.
        internal static IndexDefinition Validate(DatabaseRecord record, string indexName, string newIndexName)
        {
            if (record.IsSharded)
                throw new NotSupportedInShardingException("Renaming an index of a sharded database is not supported");

            if (TryGetRenameInProgress(record.RollingIndexes, indexName, out var renamingFrom, out var renamingTo) ||
                TryGetRenameInProgress(record.RollingIndexes, newIndexName, out renamingFrom, out renamingTo))
                throw new InvalidOperationException($"Cannot rename the index '{indexName}' to '{newIndexName}' because the rename of '{renamingFrom}' to '{renamingTo}' is in progress");

            if (record.Indexes.TryGetValue(indexName, out var definition) == false)
            {
                if (record.AutoIndexes.ContainsKey(indexName))
                    throw new InvalidOperationException($"Cannot rename the auto index '{indexName}'. Only static indexes can be renamed.");

                IndexDoesNotExistException.ThrowFor(indexName);
            }

            indexName = definition.Name;

            if (record.RollingIndexes != null && record.RollingIndexes.ContainsKey(indexName))
                throw new InvalidOperationException($"Cannot rename the index '{indexName}' because a rolling deployment of it is in progress");

            var lockMode = definition.LockMode ?? IndexLockMode.Unlock;
            if (lockMode != IndexLockMode.Unlock)
                throw new InvalidOperationException($"Cannot rename the index '{indexName}' because its lock mode is '{lockMode}'");

            if (IndexStore.IsValidIndexName(newIndexName, isStatic: true, out var errorMessage) == false)
                throw new ArgumentException(errorMessage);

            if (newIndexName.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException($"Index name '{newIndexName}' is not permitted. Index name cannot start with '{Constants.Documents.Indexing.SideBySideIndexNamePrefix}'");

            var newSafeName = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(newIndexName);
            foreach (var name in GetIndexNamesInUse(record))
            {
                if (string.Equals(name, newIndexName, StringComparison.OrdinalIgnoreCase) ||
                    // No case insensitive rename (even for the same index name) windows / macOS have case-insensitive file systems
                    string.Equals(IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(name), newSafeName, StringComparison.OrdinalIgnoreCase))
                    throw new IndexAlreadyExistException($"Cannot rename the index '{indexName}' to '{newIndexName}' because the name is in use by the index '{name}'");
            }

            return definition;
        }

        internal static bool TryGetRenameInProgress(Dictionary<string, RollingIndex> rollingIndexes, string name, out string oldName, out string newName)
        {
            oldName = null;
            newName = null;

            if (rollingIndexes == null)
                return false;

            if (rollingIndexes.TryGetValue(name, out var rollingIndex) && rollingIndex.RenamedFrom != null)
            {
                oldName = rollingIndex.RenamedFrom;
                newName = name;
                return true;
            }

            foreach (var kvp in rollingIndexes)
            {
                if (string.Equals(kvp.Value.RenamedFrom, name, StringComparison.Ordinal))
                {
                    oldName = kvp.Value.RenamedFrom;
                    newName = kvp.Key;
                    return true;
                }
            }

            return false;
        }

        public static void ThrowIfRenameInProgress(DatabaseRecord record, string indexName, bool allowNewName = false)
        {
            if (TryGetRenameInProgress(record.RollingIndexes, indexName, out var oldName, out var newName) == false)
                return;

            if (allowNewName && string.Equals(indexName, newName, StringComparison.Ordinal))
                return; // we allow deletes during an index rename, but only using the new name

            var message = $"Cannot modify the index '{indexName}' because the rename of '{oldName}' to '{newName}' is in progress";
            if (allowNewName)
                message += $". Use the new name '{newName}'.";

            throw new InvalidOperationException(message);
        }

        private static IEnumerable<string> GetIndexNamesInUse(DatabaseRecord record)
        {
            return record.Indexes.Keys.Concat(record.AutoIndexes.Keys).Concat(GetOldNamesOfRenamesInProgress(record));
        }

        // The old name of a rename in progress is in use: a node that did not rename yet has an index with that name.
        internal static IEnumerable<string> GetOldNamesOfRenamesInProgress(DatabaseRecord record)
        {
            if (record.RollingIndexes == null)
                yield break;

            foreach (var rollingIndex in record.RollingIndexes.Values)
            {
                if (rollingIndex.RenamedFrom != null)
                    yield return rollingIndex.RenamedFrom;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(NewIndexName)] = NewIndexName;
            json[nameof(CreatedAt)] = CreatedAt;
            json[nameof(RevisionsToKeep)] = RevisionsToKeep;
        }
    }
}
