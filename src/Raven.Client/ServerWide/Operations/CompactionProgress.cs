using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class CompactionProgressBase
    {
        public virtual Dictionary<string, CompactionProgressBase> IndexesResults { get; set; } = new Dictionary<string, CompactionProgressBase>();
        public virtual string Message { get; set; }
        public virtual string TreeName { get; set; }
        public virtual long TreeProgress { get; set; }
        public virtual long TreeTotal { get; set; }
        public virtual long GlobalProgress { get; set; }
        public virtual long GlobalTotal { get; set; }
        public virtual bool Skipped { get; set; }
        public virtual bool Processed { get; set; }
        
        public virtual DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(TreeName)] = TreeName,
                [nameof(TreeProgress)] = TreeProgress,
                [nameof(TreeTotal)] = TreeTotal,
                [nameof(GlobalProgress)] = GlobalProgress,
                [nameof(GlobalTotal)] = GlobalTotal,
                [nameof(Skipped)] = Skipped,
                [nameof(Processed)] = Processed
            };
            
            if (IndexesResults.Count != 0)
            {
                var indexes = new DynamicJsonValue();
                foreach (var index in IndexesResults)
                {
                    indexes[index.Key] = index.Value.ToJson();
                }

                json[nameof(IndexesResults)] = indexes;
            }
            
            return json;

        }
    }

    public class CompactionProgress : CompactionProgressBase, IOperationProgress
    {
        private readonly CompactionResult _result;

        public CompactionProgress() : this(new CompactionResult())
        {
            // used by json serialization
        }

        public CompactionProgress(CompactionResult result)
        {
            _result = result;
        }

        public override string Message
        {
            get => _result.Message;
            set => _result.Message = value;
        }

        public override string TreeName
        {
            get => _result.TreeName;
            set => _result.TreeName = value;
        }

        public override long TreeProgress
        {
            get => _result.TreeProgress;
            set => _result.TreeProgress = value;
        }

        public override long TreeTotal
        {
            get => _result.TreeTotal;
            set => _result.TreeTotal = value;
        }
        
        public override long GlobalProgress
        {
            get => _result.GlobalProgress;
            set => _result.GlobalProgress = value;
        }
        
        public override long GlobalTotal
        {
            get => _result.GlobalTotal;
            set => _result.GlobalTotal = value;
        }
        
        public override Dictionary<string, CompactionProgressBase> IndexesResults
        {
            get => _result.IndexesResults;
            set => _result.IndexesResults = value;
        }
        
        public override bool Skipped
        {
            get => _result.Skipped;
            set => _result.Skipped = value;
        }
        
        public override bool Processed
        {
            get => _result.Processed;
            set => _result.Processed = value;
        }
    }
}
