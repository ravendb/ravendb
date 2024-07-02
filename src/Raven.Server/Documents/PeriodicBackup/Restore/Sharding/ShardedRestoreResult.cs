using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.Restore.Sharding
{
    public sealed class ShardedRestoreResult : IShardedOperationResult<ShardNodeRestoreResult>
    {
        public ShardedRestoreResult()
        {
            Message = null;
        }

        public List<ShardNodeRestoreResult> Results { get; set; }

        public string Message { get; private set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()))
            };
        }

        public bool ShouldPersist => true;

        public bool CanMerge => false;
        
        public void MergeWith(IOperationResult result)
        {
            throw new NotImplementedException();
        }

        public void CombineWith(IOperationResult result, int shardNumber, string nodeTag)
        {
            Results ??= new List<ShardNodeRestoreResult>();

            if (result is not RestoreResult restoreResult)
                return;

            Results.Add(new ShardNodeRestoreResult
            {
                Result = restoreResult,
                ShardNumber = shardNumber,
                NodeTag = nodeTag
            });
        }
    }

    public sealed class ShardNodeRestoreResult : ShardNodeOperationResult<RestoreResult>
    {
        public override bool ShouldPersist => true;
    }

    public sealed class ShardedRestoreProgress : RestoreProgress, IShardedOperationProgress
    {
        public int ShardNumber { get; set; }

        public string NodeTag { get; set; }

        public void Fill(IOperationProgress progress, int shardNumber, string nodeTag)
        {
            ShardNumber = shardNumber;
            NodeTag = nodeTag;

            if (progress is not RestoreProgress rp)
                return;

            _result = rp._result;
            DatabaseRecord = rp.DatabaseRecord;
            Documents = rp.Documents;
            RevisionDocuments = rp.RevisionDocuments;
            Tombstones = rp.Tombstones;
            Conflicts = rp.Conflicts;
            Identities = rp.Identities;
            Indexes = rp.Indexes;
            CompareExchange = rp.CompareExchange;
            Subscriptions = rp.Subscriptions;
            ReplicationHubCertificates = rp.ReplicationHubCertificates;
            Counters = rp.Counters;
            TimeSeries = rp.TimeSeries;
            CompareExchangeTombstones = rp.CompareExchangeTombstones;
            TimeSeriesDeletedRanges = rp.TimeSeriesDeletedRanges;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ShardNumber)] = ShardNumber;
            json[nameof(NodeTag)] = NodeTag;
            return json;
        }
    }
}
