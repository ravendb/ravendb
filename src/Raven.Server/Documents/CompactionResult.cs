using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using Lucene.Net.Messages;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class CompactionResult : CompactionProgressBase, IOperationResult, IOperationProgress
    {
        public long SizeBeforeCompactionInMb { get; set; }
        public long SizeAfterCompactionInMb { get; set; }
        private readonly List<string> _messages;

        public CompactionResult(string name)
        {
            Name = string.IsNullOrEmpty(name) ? string.Empty : name;
            _messages = new List<string>();
            Progress = new CompactionProgress(this);
            IndexesResults = new Dictionary<string, CompactionProgressBase>();
        }

        public override string Message { get; set; }

        public CompactionProgress Progress { get; }

        public string Name { get; }

        public Dictionary<string, CompactionProgressBase> IndexesResults { get; }

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
            json[nameof(Name)] = Name;
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
