using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using Lucene.Net.Messages;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public abstract class CompactionResultBase
    {
        public virtual string Name { get; set; }
        public virtual long SizeBeforeCompactionInMb { get; set; }
        public virtual long SizeAfterCompactionInMb { get; set; }
        public virtual long TreeTotal { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Name)] = Name,
                [nameof(SizeBeforeCompactionInMb)] = SizeBeforeCompactionInMb,
                [nameof(SizeAfterCompactionInMb)] = SizeAfterCompactionInMb,
                [nameof(TreeTotal)] = TreeTotal
            };
        }
    }

    public class CompactionResult : CompactionProgressBase, IOperationResult, IOperationProgress
    {
        private readonly string _name;
        public long SizeBeforeCompactionInMb { get; set; }
        public long SizeAfterCompactionInMb { get; set; }
        private readonly List<string> _messages;
        protected CompactionProgress _progress;
        protected readonly Dictionary<string, CompactionProgressBase> _indexesResults;

        public CompactionResult(string name)
        {
            _name = string.IsNullOrEmpty(name) ? string.Empty : name;
            _messages = new List<string>();
            _progress = new CompactionProgress(this);
            _indexesResults = new Dictionary<string, CompactionProgressBase>();
        }

        public override string Message { get; set; }

        public CompactionProgress Progress => _progress;
        public string Name => _name;
        public Dictionary<string, CompactionProgressBase> IndexesResults => _indexesResults;

        public IReadOnlyList<string> Messages => _messages;

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
            _messages.Add(Message);
        }

        private void AddMessage(string type, string message)
        {
            Message = $"[{SystemTime.UtcNow:T} {type}] {message}";
            _messages.Add(Message);
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Name)] = _name;
            json[nameof(SizeBeforeCompactionInMb)] = SizeBeforeCompactionInMb;
            json[nameof(SizeAfterCompactionInMb)] = SizeAfterCompactionInMb;
            json[nameof(Messages)] = Messages;

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

        public bool ShouldPersist => false;
    }
}
