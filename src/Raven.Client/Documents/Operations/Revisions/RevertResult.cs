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

        public void Warn(string id, string message)
        {
            Warnings[id] = message;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(ScannedRevisions)] = ScannedRevisions,
                [nameof(ScannedDocuments)] = ScannedDocuments,
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

    public class EnforceConfigurationResult : OperationResult
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

    public class RevertResult : OperationResult
    {
        public int RevertedDocuments { get; set; }

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
                ScannedDocuments = ScannedDocuments,
                ScannedRevisions = ScannedRevisions,
                Warnings = Warnings
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RevertedDocuments)] = RevertedDocuments;
            return json;
        }
    }
}
