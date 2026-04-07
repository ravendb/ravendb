using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public abstract class OperationResult : IOperationResult, IOperationProgress
    {
        public int ScannedRevisions { get; set; }
        public int ScannedDocuments { get; set; }
        public Dictionary<string, string> Warnings { get; set; } = new Dictionary<string, string>();
        public string Message { get; }

        /// <summary>
        /// The highest etag among the last-scanned etags across all collections on each database.
        /// When multiple collections are processed, this is the maximum of the per-collection
        /// last-scanned etags. Pass this value as <c>StartFromEtags</c> to resume the operation.
        /// For sharded databases the key is the shard database name (e.g. <c>"Northwind$0"</c>);
        /// for non-sharded databases it is the database name (e.g. <c>"Northwind"</c>).
        /// </summary>
        public Dictionary<string, long> LastProcessedEtags { get; set; } = new();

        /// <summary>
        /// The etag barriers used during this run, keyed by database name.
        /// For sharded databases the key is the shard database name (e.g. <c>"Northwind$0"</c>);
        /// for non-sharded databases it is the database name (e.g. <c>"Northwind"</c>).
        /// </summary>
        public Dictionary<string, long> EtagBarriersUsed { get; set; } = new();

        /// <summary>
        /// The node tags that processed each database/shard during this run.
        /// For sharded databases the key is the shard database name (e.g. <c>"Northwind$0"</c>);
        /// for non-sharded databases it is the database name (e.g. <c>"Northwind"</c>).
        /// Pass this value into <see cref="RevisionsOperationContinuationParameters.NodeTags"/>
        /// when resuming the operation to ensure it runs on the same node(s).
        /// </summary>
        public Dictionary<string, string> NodeTags { get; set; } = new();

        public void Warn(string id, string message)
        {
            Warnings[id] = message;
        }

        public virtual DynamicJsonValue ToJson()
        {
            var lastProcessedEtags = new DynamicJsonValue();
            foreach (var kvp in LastProcessedEtags)
                lastProcessedEtags[kvp.Key] = kvp.Value;

            var etagBarriersUsed = new DynamicJsonValue();
            foreach (var kvp in EtagBarriersUsed)
                etagBarriersUsed[kvp.Key] = kvp.Value;

            var nodeTags = new DynamicJsonValue();
            foreach (var kvp in NodeTags)
                nodeTags[kvp.Key] = kvp.Value;

            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(ScannedRevisions)] = ScannedRevisions,
                [nameof(ScannedDocuments)] = ScannedDocuments,
                [nameof(LastProcessedEtags)] = lastProcessedEtags,
                [nameof(EtagBarriersUsed)] = etagBarriersUsed,
                [nameof(NodeTags)] = nodeTags,
                [nameof(Warnings)] = DynamicJsonValue.Convert(Warnings)
            };
        }

        public virtual IOperationProgress Clone()
        {
            throw new System.NotImplementedException();
        }

        public virtual void MergeWith(IOperationProgress progress)
        {
            if (progress is not OperationResult r)
                return;

            ScannedDocuments += r.ScannedDocuments;
            ScannedRevisions += r.ScannedRevisions;
            foreach (var kvp in r.LastProcessedEtags)
                LastProcessedEtags[kvp.Key] = kvp.Value;
            foreach (var kvp in r.EtagBarriersUsed)
                EtagBarriersUsed[kvp.Key] = kvp.Value;
            foreach (var kvp in r.NodeTags)
                NodeTags[kvp.Key] = kvp.Value;
            foreach (var warning in r.Warnings)
            {
                try
                {
                    Warnings.Add(warning.Key, warning.Value);
                }
                catch
                {
                    //
                }
            }
        }

        public virtual bool CanMerge => false;

        public virtual void MergeWith(IOperationResult result)
        {
            if (result is not OperationResult r)
                return;

            ScannedDocuments += r.ScannedDocuments;
            ScannedRevisions += r.ScannedRevisions;
            foreach (var kvp in r.LastProcessedEtags)
                LastProcessedEtags[kvp.Key] = kvp.Value;
            foreach (var kvp in r.EtagBarriersUsed)
                EtagBarriersUsed[kvp.Key] = kvp.Value;
            foreach (var kvp in r.NodeTags)
                NodeTags[kvp.Key] = kvp.Value;
            foreach (var warning in r.Warnings)
            {
                try
                {
                    Warnings.Add(warning.Key, warning.Value);
                }
                catch
                {
                    //
                }
            }
        }

        public bool ShouldPersist => false;
    }

    public sealed class EnforceConfigurationResult : OperationResult
    {
        public int RemovedRevisions { get; set; }

        public override bool CanMerge => true;

        public override void MergeWith(IOperationResult result)
        {
            if (result is not EnforceConfigurationResult r)
                return;

            RemovedRevisions += r.RemovedRevisions;

            base.MergeWith(result);
        }

        public override void MergeWith(IOperationProgress progress)
        {
            if (progress is not EnforceConfigurationResult r)
                return;

            RemovedRevisions += r.RemovedRevisions;

            base.MergeWith(progress);
        }

        public override IOperationProgress Clone()
        {
            return new EnforceConfigurationResult()
            {
                RemovedRevisions = RemovedRevisions,
                ScannedDocuments = ScannedDocuments,
                ScannedRevisions = ScannedRevisions,
                LastProcessedEtags = new Dictionary<string, long>(LastProcessedEtags),
                EtagBarriersUsed = new Dictionary<string, long>(EtagBarriersUsed),
                NodeTags = new Dictionary<string, string>(NodeTags),
                Warnings = Warnings
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RemovedRevisions)] = RemovedRevisions;
            return json;
        }
    }

    public sealed class AdoptOrphanedRevisionsResult : OperationResult
    {
        public int AdoptedCount { get; set; }

        public override bool CanMerge => true;

        public override void MergeWith(IOperationResult result)
        {
            if (result is not AdoptOrphanedRevisionsResult r)
                return;

            AdoptedCount += r.AdoptedCount;

            base.MergeWith(result);
        }

        public override void MergeWith(IOperationProgress progress)
        {
            if (progress is not AdoptOrphanedRevisionsResult r)
                return;

            AdoptedCount += r.AdoptedCount;

            base.MergeWith(progress);
        }

        public override IOperationProgress Clone()
        {
            return new AdoptOrphanedRevisionsResult()
            {
                AdoptedCount = AdoptedCount,
                ScannedDocuments = ScannedDocuments,
                ScannedRevisions = ScannedRevisions,
                LastProcessedEtags = new Dictionary<string, long>(LastProcessedEtags),
                EtagBarriersUsed = new Dictionary<string, long>(EtagBarriersUsed),
                NodeTags = new Dictionary<string, string>(NodeTags),
                Warnings = Warnings
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(AdoptedCount)] = AdoptedCount;
            return json;
        }
    }

    public sealed class RevertResult : OperationResult
    {
        public int RevertedDocuments { get; set; }
        private int _failedCollections { get; set; }

        public override bool CanMerge => true;

        public override void MergeWith(IOperationResult result)
        {
            if (result is not RevertResult r)
                return;

            RevertedDocuments += r.RevertedDocuments;

            base.MergeWith(result);
        }

        public override void MergeWith(IOperationProgress progress)
        {
            if (progress is not RevertResult r)
                return;

            RevertedDocuments += r.RevertedDocuments;

            base.MergeWith(progress);
        }

        public override IOperationProgress Clone()
        {
            return new RevertResult()
            {
                RevertedDocuments = RevertedDocuments,
                EtagBarriersUsed = new Dictionary<string, long>(EtagBarriersUsed),
                ScannedDocuments = ScannedDocuments,
                ScannedRevisions = ScannedRevisions,
                LastProcessedEtags = new Dictionary<string, long>(LastProcessedEtags),
                NodeTags = new Dictionary<string, string>(NodeTags),
                Warnings = Warnings
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RevertedDocuments)] = RevertedDocuments;
            return json;
        }

        public void WarnAboutFailedCollection(string message)
        {
            var id = $"Revert Collection Fail No.{++_failedCollections}";
            Warn(id, message);
        }
    }
}
