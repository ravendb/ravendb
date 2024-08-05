using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler
{
    public class SmugglerResult : SmugglerProgressBase, IOperationResult
    {
        private readonly object _locker = new();

        private List<string> _messages;
        protected SmugglerProgress _progress;
        private readonly Stopwatch _sw;
        private DatabaseItemType _itemTypeInProgress;
        
        public SmugglerResult()
        {
            _sw = Stopwatch.StartNew();
            _messages = new List<string>();

            /*
            *  NOTE:
            *
            *  About to add new/change property below?
            *
            *  Please remember to include this property in SmugglerProgress class
            */

            DatabaseRecord = new DatabaseRecordProgress();
            Documents = new CountsWithSkippedCountAndLastEtagAndAttachments();
            RevisionDocuments = new CountsWithSkippedCountAndLastEtagAndAttachments();
            Tombstones = new CountsWithLastEtag();
            Conflicts = new CountsWithLastEtag();
            Identities = new CountsWithLastEtag();
            Indexes = new Counts();
            CompareExchange = new CountsWithLastEtag();
            Counters = new CountsWithSkippedCountAndLastEtag();
            CompareExchangeTombstones = new Counts();
            Subscriptions = new Counts();
            ReplicationHubCertificates = new Counts();
            TimeSeries = new CountsWithSkippedCountAndLastEtag();
            TimeSeriesDeletedRanges = new CountsWithSkippedCountAndLastEtag();
            _progress = new SmugglerProgress(this);
        }

        public string Message { get; private set; }

        public TimeSpan Elapsed => _sw.Elapsed;

        public IOperationProgress Progress => _progress;

        bool IOperationResult.CanMerge => true;

        void IOperationResult.MergeWith(IOperationResult result)
        {
            if (result is SmugglerResult smugglerResult == false)
                throw new InvalidOperationException();

            smugglerResult.Documents.SkippedCount += Documents.SkippedCount;
            smugglerResult.Documents.ReadCount += Documents.ReadCount;
            smugglerResult.Documents.ErroredCount += Documents.ErroredCount;
            smugglerResult.Documents.SizeInBytes += Documents.SizeInBytes;

            smugglerResult.Documents.LastEtag = Math.Max(smugglerResult.Documents.LastEtag, Documents.LastEtag);
            smugglerResult.Documents.Attachments.ReadCount += Documents.Attachments.ReadCount;
            smugglerResult.Documents.Attachments.SizeInBytes += Documents.Attachments.SizeInBytes;

            smugglerResult.RevisionDocuments.ReadCount += RevisionDocuments.ReadCount;
            smugglerResult.RevisionDocuments.ErroredCount += RevisionDocuments.ErroredCount;
            smugglerResult.RevisionDocuments.SizeInBytes += RevisionDocuments.SizeInBytes;
            smugglerResult.RevisionDocuments.LastEtag = Math.Max(smugglerResult.RevisionDocuments.LastEtag, RevisionDocuments.LastEtag);
            smugglerResult.RevisionDocuments.Attachments = RevisionDocuments.Attachments;
            smugglerResult.RevisionDocuments.Attachments.ReadCount += RevisionDocuments.Attachments.ReadCount;
            smugglerResult.RevisionDocuments.Attachments.SizeInBytes += RevisionDocuments.Attachments.SizeInBytes;

            smugglerResult.Counters.ReadCount += Counters.ReadCount;
            smugglerResult.Counters.ErroredCount += Counters.ErroredCount;
            smugglerResult.Counters.LastEtag = Math.Max(smugglerResult.Counters.LastEtag, Counters.LastEtag);

            smugglerResult.TimeSeries.ReadCount += TimeSeries.ReadCount;
            smugglerResult.TimeSeries.ErroredCount += TimeSeries.ErroredCount;
            smugglerResult.TimeSeries.SizeInBytes += TimeSeries.SizeInBytes;

            smugglerResult.TimeSeries.LastEtag = Math.Max(smugglerResult.TimeSeries.LastEtag, TimeSeries.LastEtag);

            smugglerResult.TimeSeriesDeletedRanges.ReadCount += TimeSeriesDeletedRanges.ReadCount;
            smugglerResult.TimeSeriesDeletedRanges.ErroredCount += TimeSeriesDeletedRanges.ErroredCount;
            smugglerResult.TimeSeriesDeletedRanges.SizeInBytes += TimeSeriesDeletedRanges.SizeInBytes;
            smugglerResult.TimeSeriesDeletedRanges.LastEtag = Math.Max(smugglerResult.TimeSeriesDeletedRanges.LastEtag, TimeSeriesDeletedRanges.LastEtag);

            smugglerResult.Identities.ReadCount += Identities.ReadCount;
            smugglerResult.Identities.ErroredCount += Identities.ErroredCount;

            smugglerResult.CompareExchange.ReadCount += CompareExchange.ReadCount;
            smugglerResult.CompareExchange.ErroredCount += CompareExchange.ErroredCount;

            smugglerResult.Subscriptions.ReadCount += Subscriptions.ReadCount;
            smugglerResult.Subscriptions.ErroredCount += Subscriptions.ErroredCount;

            smugglerResult.Indexes.ReadCount += Indexes.ReadCount;
            smugglerResult.Indexes.ErroredCount += Indexes.ErroredCount;

            foreach (var message in Messages)
                smugglerResult.AddMessage(message);
        }

        public IReadOnlyList<string> Messages
        {
            get
            {
                lock (_locker)
                {
                    return _messages.ToArray();
                }
            }

            set
            {
                lock (_locker)
                {
                    _messages = value.ToList();
                }
            }
        }

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

        public void StartProcessingForType(DatabaseItemType type)
        {
            _itemTypeInProgress = type;
            AddInfo($"Started processing {_itemTypeInProgress}.");
        }

        public void StopProcessingActualType(Counts counts)
        {
            AddInfo($"Finished processing {_itemTypeInProgress}. {counts}");
            _itemTypeInProgress = DatabaseItemType.None;
        }

        public void OnCorruptedData(object sender, InvalidOperationException e)
        {
            switch (_itemTypeInProgress)
            {
                case DatabaseItemType.Documents:
                    Documents.ErroredCount++;
                    break;
                case DatabaseItemType.RevisionDocuments:
                    RevisionDocuments.ErroredCount++;
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"Handling corrupted data of such DatabaseItemType '{nameof(_itemTypeInProgress)}' is not provided for.", e);
            }

            AddError(e.Message);
        }

        internal void AddMessage(string message)
        {
            Message = message;

            lock (_locker)
            {
                _messages.Add(Message);
            }
        }

        private void AddMessage(string type, string message)
        {
            Message = $"[{SystemTime.UtcNow:T} {type}] {message}";

            lock (_locker)
            {
                _messages.Add(Message);
            }
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

        public string LegacyLastDocumentEtag { get; set; }
        public string LegacyLastAttachmentEtag { get; set; }

        public class SmugglerProgress : SmugglerProgressBase
        {
            internal SmugglerResult _result;

            public SmugglerProgress()
                : this(null)
            {
                // for deserialization
            }

            public SmugglerProgress(SmugglerResult result)
            {
                _result = result;
                Message = _result?.Message;
                DatabaseRecord = _result?.DatabaseRecord;
                Documents = _result?.Documents;
                RevisionDocuments = _result?.RevisionDocuments;
                Tombstones = _result?.Tombstones;
                Conflicts = _result?.Conflicts;
                Identities = _result?.Identities;
                Indexes = _result?.Indexes;
                CompareExchange = _result?.CompareExchange;
                Counters = _result?.Counters;
                CompareExchangeTombstones = _result?.CompareExchangeTombstones;
                Subscriptions = _result?.Subscriptions;
                TimeSeries = _result?.TimeSeries;
                ReplicationHubCertificates = _result?.ReplicationHubCertificates;
                TimeSeriesDeletedRanges = _result?.TimeSeriesDeletedRanges;
            }

            internal string Message { get; set; }

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(Message)] = _result?.Message ?? Message;
                return json;
            }
        }

        public long GetLastEtag()
        {
            var lastEtag = Documents.LastEtag;

            if (RevisionDocuments.LastEtag > lastEtag)
                lastEtag = RevisionDocuments.LastEtag;

            if (Tombstones.LastEtag > lastEtag)
                lastEtag = Tombstones.LastEtag;

            if (Conflicts.LastEtag > lastEtag)
                lastEtag = Conflicts.LastEtag;

            if (Counters.LastEtag > lastEtag)
                lastEtag = Counters.LastEtag;

            if (TimeSeries.LastEtag > lastEtag)
                lastEtag = TimeSeries.LastEtag;

            if (TimeSeriesDeletedRanges.LastEtag > lastEtag)
                lastEtag = TimeSeriesDeletedRanges.LastEtag;

            return lastEtag;
        }

        public long GetLastRaftIndex()
        {
            var lastEtag = Identities.LastEtag;

            if (CompareExchange.LastEtag > lastEtag)
                lastEtag = CompareExchange.LastEtag;

            return lastEtag;
        }
    }
}
