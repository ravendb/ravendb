using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler
{
    public class SmugglerResult : SmugglerProgressBase, IOperationResult
    {
        private readonly List<string> _messages;
        protected SmugglerProgress _progress;
        private readonly Stopwatch _sw;

        public SmugglerResult()
        {
            _sw = Stopwatch.StartNew();
            _messages = new List<string>();
            _progress = new SmugglerProgress(this);

            Documents = new CountsWithSkippedCountAndLastEtag();
            RevisionDocuments = new CountsWithLastEtag();
            Identities = new Counts();
            Indexes = new Counts();
        }

        public string Message { get; private set; }

        public TimeSpan Elapsed => _sw.Elapsed;

        public IOperationProgress Progress => _progress;

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
            _sw.Stop();

            var json = base.ToJson();
            json[nameof(Messages)] = Messages;
            json[nameof(Elapsed)] = Elapsed;

            return json;
        }

        public bool ShouldPersist => true;

        public class SmugglerProgress : SmugglerProgressBase, IOperationProgress
        {
            private readonly SmugglerResult _result;

            public SmugglerProgress(SmugglerResult result)
            {
                _result = result;
            }

            private string Message => _result.Message;

            public override CountsWithSkippedCountAndLastEtag Documents => _result.Documents;
            public override CountsWithLastEtag RevisionDocuments => _result.RevisionDocuments;
            public override Counts Identities => _result.Identities;
            public override Counts Indexes => _result.Indexes;

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(Message)] = Message;
                return json;
            }
        }

        public long GetLastEtag()
        {
            var lastEtag = Documents.LastEtag;
            if (RevisionDocuments.LastEtag > lastEtag)
                lastEtag = RevisionDocuments.LastEtag;

            //TODO: take into account the last tombstones etag
            //TODO: take into account the last conflicts etag

            return lastEtag;
        }
    }

    public abstract class SmugglerProgressBase
    {
        public virtual CountsWithSkippedCountAndLastEtag Documents { get; set; }

        public virtual CountsWithLastEtag RevisionDocuments { get; set; }

        public virtual Counts Identities { get; set; }

        public virtual Counts Indexes { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Documents)] = Documents.ToJson(),
                [nameof(RevisionDocuments)] = RevisionDocuments.ToJson(),
                [nameof(Identities)] = Identities.ToJson(),
                [nameof(Indexes)] = Indexes.ToJson(),
            };
        }

        public class Counts
        {
            public bool Skipped { get; set; }

            public bool Processed { get; set; }

            public long ReadCount { get; set; }

            public long ErroredCount { get; set; }

            public virtual DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Processed)] = Processed,
                    [nameof(Skipped)] = Skipped,
                    [nameof(ReadCount)] = ReadCount,
                    [nameof(ErroredCount)] = ErroredCount
                };
            }

            public override string ToString()
            {
                return $"Read: {ReadCount}. Errored: {ErroredCount}.";
            }
        }

        public class CountsWithLastEtag : Counts
        {
            public long LastEtag { get; set; }

            public Counts Attachments { get; set; } = new Counts();

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(LastEtag)] = LastEtag;
                json[nameof(Attachments)] = Attachments.ToJson();
                return json;
            }

            public override string ToString()
            {
                return $"{base.ToString()} Attachments: {Attachments}";
            }
        }

        public class CountsWithSkippedCountAndLastEtag : CountsWithLastEtag
        {
            public long SkippedCount { get; set; }

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(SkippedCount)] = SkippedCount;
                return json;
            }

            public override string ToString()
            {
                return $"Skipped: {SkippedCount}. {base.ToString()}";
            }
        }
    }
}
