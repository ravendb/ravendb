using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class SetIndexLockCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexLockMode LockMode;

        public SetIndexLockCommand() : base(null)
        {
            // for deserialization
        }

        public SetIndexLockCommand(string name, IndexLockMode mode, string databaseName)
            : base(databaseName)
        {
            IndexName = name;
            LockMode = mode;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Indexes.TryGetValue(IndexName, out IndexDefinition staticIndex))
            {
                staticIndex.LockMode = LockMode;
            }

            if (record.AutoIndexes.ContainsKey(IndexName))
            {
                throw new RachisApplyException($"'Lock Mode' can't be set for the Auto-Index '{IndexName}'.");
            }

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(LockMode)] = LockMode;
        }
    }
}
