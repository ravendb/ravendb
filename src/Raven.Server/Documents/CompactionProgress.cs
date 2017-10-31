using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class CompactionProgressBase
    {
        public virtual string Message { get; set; }
        public virtual string TreeName { get; set; }
        public virtual long TreeProgress { get; set; }
        public virtual long TreeTotal { get; set; }
        public virtual long GlobalProgress { get; set; }
        public virtual long GlobalTotal { get; set; }
        
        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(TreeName)] = TreeName,
                [nameof(TreeProgress)] = TreeProgress,
                [nameof(TreeTotal)] = TreeTotal,
                [nameof(GlobalProgress)] = GlobalProgress,
                [nameof(GlobalTotal)] = GlobalTotal
            };

        }
    }

    public class CompactionProgress : CompactionProgressBase, IOperationProgress
    {
        private readonly CompactionResult _result;

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
    }
}
