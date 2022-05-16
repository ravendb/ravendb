using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public class MigrationResult : IOperationResult
    {
        private readonly List<string> _messages;
        protected MigrationProgress _progress;
        private readonly Stopwatch _sw;
        public bool ShouldPersist => true;

        bool IOperationResult.CanMerge => false;

        void IOperationResult.MergeWith(IOperationResult result)
        {
            throw new NotImplementedException();
        }

        public MigrationResult(MigrationSettings settings)
        {
            _sw = Stopwatch.StartNew();
            _messages = new List<string>();
            _progress = new MigrationProgress(this);

            PerCollectionCount = new Dictionary<string, Counts>();

            foreach (var collection in settings.Collections)
            {
                PerCollectionCount[collection.Name] = new Counts();
            }
        }

        public Dictionary<string, Counts> PerCollectionCount { get; set; }

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

        public DynamicJsonValue ToJson()
        {
            _sw.Stop();

            return new DynamicJsonValue(GetType())
            {
                [nameof(PerCollectionCount)] = DynamicJsonValue.Convert(PerCollectionCount),
                [nameof(Messages)] = Messages,
                [nameof(Elapsed)] = Elapsed
            };
        }
    }

    public class MigrationProgress : IOperationProgress
    {
        protected readonly MigrationResult _result;

        public MigrationProgress(MigrationResult result)
        {
            _result = result;
        }

        private string Message => _result.Message;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(MigrationResult.PerCollectionCount)] = DynamicJsonValue.Convert(_result.PerCollectionCount)
            };
        }

        IOperationProgress IOperationProgress.Clone()
        {
            throw new NotImplementedException();
        }

        bool IOperationProgress.CanMerge => false;

        void IOperationProgress.MergeWith(IOperationProgress progress)
        {
            throw new NotImplementedException();
        }
    }

    public class Counts : IDynamicJson
    {
        public bool Processed { get; set; }

        public long ReadCount { get; set; }

        public long ErroredCount { get; set; }

        public long SkippedCount { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Processed)] = Processed,
                [nameof(ReadCount)] = ReadCount,
                [nameof(SkippedCount)] = SkippedCount,
                [nameof(ErroredCount)] = ErroredCount
            };
        }

        public override string ToString()
        {
            return $"Skipped: {SkippedCount}. " +
                   $"Read: {ReadCount}. " +
                   $"Errored: {ErroredCount}.";
        }
    }
}
