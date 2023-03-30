using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups.Sharding
{
    public class ShardedRestoreSettings : IDynamicJson
    {
        public Dictionary<int, SingleShardRestoreSetting> Shards { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Shards)] = Shards != null ? DynamicJsonValue.Convert(Shards) : null
            };
        }

        public ShardedRestoreSettings()
        {
        }

        internal ShardedRestoreSettings(ShardedRestoreSettings other)
        {
            if (other == null)
                throw new ArgumentException(nameof(other));

            Shards = new Dictionary<int, SingleShardRestoreSetting>(other.Shards.Count);
            foreach (var shardToSetting in other.Shards)
            {
                Shards[shardToSetting.Key] = new SingleShardRestoreSetting(shardToSetting.Value);
            }
        }
    }

    public class SingleShardRestoreSetting : IDynamicJson
    {
        public int ShardNumber { get; set; }
        public string NodeTag { get; set; }
        public string FolderName { get; set; }
        public string LastFileNameToRestore { get; set; }

        public SingleShardRestoreSetting()
        {
        }

        internal SingleShardRestoreSetting(SingleShardRestoreSetting other)
        {
            ShardNumber = other.ShardNumber;
            NodeTag = other.NodeTag;
            FolderName = other.FolderName;
            LastFileNameToRestore = other.LastFileNameToRestore;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ShardNumber)] = ShardNumber,
                [nameof(NodeTag)] = NodeTag,
                [nameof(FolderName)] = FolderName,
                [nameof(LastFileNameToRestore)] = LastFileNameToRestore
            };
        }
    }
}
