using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class CompactionResult : CompactionProgressBase, IOperationResult, IOperationProgress
    {
        public long SizeBeforeCompactionInMb { get; set; }
        public long SizeAfterCompactionInMb { get; set; }
        private readonly ConcurrentQueue<string> _messages;

        public CompactionResult() : this(string.Empty)
        {
            // used by json serialization
        }
        
        public CompactionResult(string name)
        {
            Name = string.IsNullOrEmpty(name) ? string.Empty : name;
            _messages = new ConcurrentQueue<string>();
            Progress = new CompactionProgress(this);
        }

        public override string Message { get; set; }

        public CompactionProgress Progress { get; }

        public string Name { get; }

        public IEnumerable<string> Messages => _messages;

        public void AddWarning(string message)
        {
            AddMessage("WARNING", message);
        }

        public void AddInfo(string message)
        {
            AddMessage("INFO", message);
        }

        public void AddError(string message)
        {
            AddMessage("ERROR", message);
        }

        internal void AddMessage(string message)
        {
            Message = message;
            _messages.Enqueue(Message);
        }

        private void AddMessage(string type, string message)
        {
            Message = $"[{SystemTime.UtcNow:T} {type}] {message}";
            _messages.Enqueue(Message);
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Name)] = Name;
            json[nameof(SizeBeforeCompactionInMb)] = SizeBeforeCompactionInMb;
            json[nameof(SizeAfterCompactionInMb)] = SizeAfterCompactionInMb;
            json[nameof(Messages)] = Messages;
            
            return json;
        }

        public bool ShouldPersist => false;
    }
}
