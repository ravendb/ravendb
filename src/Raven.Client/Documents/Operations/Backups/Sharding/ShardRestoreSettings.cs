using System;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups.Sharding
{
    public class ShardRestoreSettings : IDynamicJson
    {
        public SingleShardRestoreSetting[] Shards { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Shards)] = Shards != null ? new DynamicJsonArray(Shards.Select(x => x.ToJson()))
                    : null
            };
        }

        public ShardRestoreSettings()
        {
        }

        internal ShardRestoreSettings(ShardRestoreSettings other)
        {
            if (other == null)
                throw new ArgumentException(nameof(other));

            Shards = new SingleShardRestoreSetting[other.Shards.Length];
            for (int i = 0; i < other.Shards.Length; i++)
            {
                Shards[i] = new SingleShardRestoreSetting(other.Shards[i]);
            }
        }
    }

    public class SingleShardRestoreSetting : IDynamicJson
    {
        public int ShardNumber { get; set; }
        public string NodeTag { get; set; }
        public string BackupPath { get; set; }

        public SingleShardRestoreSetting()
        {
        }

        internal SingleShardRestoreSetting(SingleShardRestoreSetting other)
        {
            ShardNumber = other.ShardNumber;
            NodeTag = other.NodeTag;
            BackupPath = other.BackupPath;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ShardNumber)] = ShardNumber,
                [nameof(NodeTag)] = NodeTag,
                [nameof(BackupPath)] = BackupPath
            };
        }
    }
}
