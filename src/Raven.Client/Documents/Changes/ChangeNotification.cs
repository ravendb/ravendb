// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Changes
{
    public class TopologyChange : DatabaseChange
    {
        public string Url;
        public string Database;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Url)] = Url,
                [nameof(Database)] = Database
            };
        }

        internal static TopologyChange FromJson(BlittableJsonReaderObject value)
        {
            value.TryGet(nameof(Url), out string url);
            value.TryGet(nameof(Database), out string database);

            return new TopologyChange
            {
                Url = url,
                Database = database
            };
        }
    }

    public class DocumentChange : DatabaseChange
    {
        /// <summary>
        /// Type of change that occurred on document.
        /// </summary>
        public DocumentChangeTypes Type { get; set; }

        /// <summary>
        /// Identifier of document for which notification was created.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Document collection name.
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// Document change vector
        /// </summary>
        public string ChangeVector { get; set; }

        internal bool TriggeredByReplicationThread;

        public override string ToString()
        {
            return $"{Type} on {Id}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type.ToString(),
                [nameof(Id)] = Id,
                [nameof(CollectionName)] = CollectionName,
                [nameof(ChangeVector)] = ChangeVector
            };
        }

        internal static DocumentChange FromJson(BlittableJsonReaderObject value)
        {
            value.TryGet(nameof(CollectionName), out string collectionName);
            value.TryGet(nameof(ChangeVector), out string changeVector);
            value.TryGet(nameof(Id), out string id);
            value.TryGet(nameof(Type), out string type);

            return new DocumentChange
            {
                CollectionName = collectionName,
                ChangeVector = changeVector,
                Id = id,
                Type = (DocumentChangeTypes)Enum.Parse(typeof(DocumentChangeTypes), type, ignoreCase: true)
            };
        }
    }

    [Flags]
    public enum DocumentChangeTypes
    {
        None = 0,

        Put = 1,
        Delete = 2,
        BulkInsertStarted = 4,
        BulkInsertEnded = 8,
        BulkInsertError = 16,
        DeleteOnTombstoneReplication = 32,
        Conflict = 64,

        Common = Put | Delete
    }

    [Flags]
    public enum IndexChangeTypes
    {
        None = 0,

        BatchCompleted = 1,

        IndexAdded = 1 << 3,
        IndexRemoved = 1 << 4,

        IndexDemotedToIdle = 1 << 5,
        IndexPromotedFromIdle = 1 << 6,

        IndexDemotedToDisabled = 1 << 8,

        IndexMarkedAsErrored = 1 << 9,

        SideBySideReplace = 1 << 10,

        Renamed = 1 << 11,
        IndexPaused = 1 << 12,
        LockModeChanged = 1 << 13,
        PriorityChanged = 1 << 14,

        RollingIndexChanged = 1 << 16
    }

    [Flags]
    public enum CounterChangeTypes
    {
        None = 0,
        Put = 1,
        Delete = 2,
        Increment = 4
    }

    public class CounterChange : DatabaseChange
    {
        /// <summary>
        /// Counter name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Counter value.
        /// </summary>
        public long Value { get; set; }

        /// <summary>
        /// Counter document identifier.
        /// </summary>
        public string DocumentId { get; set; }

        /// <summary>
        /// Document collection name.
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// Counter change vector.
        /// </summary>
        public string ChangeVector { get; set; }

        /// <summary>
        /// Type of change that occurred on counter.
        /// </summary>
        public CounterChangeTypes Type { get; set; }

        internal bool TriggeredByReplicationThread;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Value)] = Value,
                [nameof(DocumentId)] = DocumentId,
                [nameof(CollectionName)] = CollectionName,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString()
            };
        }

        internal static CounterChange FromJson(BlittableJsonReaderObject value)
        {
            value.TryGet(nameof(Name), out string name);
            value.TryGet(nameof(Value), out long val);
            value.TryGet(nameof(DocumentId), out string documentId);
            value.TryGet(nameof(CollectionName), out string collectionName);
            value.TryGet(nameof(ChangeVector), out string changeVector);
            value.TryGet(nameof(Type), out string type);

            return new CounterChange
            {
                Name = name,
                Value = val,
                DocumentId = documentId,
                ChangeVector = changeVector,
                CollectionName = collectionName,
                Type = (CounterChangeTypes)Enum.Parse(typeof(CounterChangeTypes), type, ignoreCase: true)
            };
        }
    }

    [Flags]
    public enum TimeSeriesChangeTypes
    {
        None = 0,
        Put = 1,
        Delete = 2,
        Mixed = 3
    }

    public class TimeSeriesChange : DatabaseChange
    {
        /// <summary>
        /// Time Series name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Apply values of time series from date.
        /// </summary>
        public DateTime From { get; set; }

        /// <summary>
        /// Apply values of time series to date.
        /// </summary>
        public DateTime To { get; set; }

        /// <summary>
        /// Time series document identifier.
        /// </summary>
        public string DocumentId { get; set; }

        /// <summary>
        /// Time series change vector.
        /// </summary>
        public string ChangeVector { get; set; }

        /// <summary>
        /// Type of change that occurred on time series.
        /// </summary>
        public TimeSeriesChangeTypes Type { get; set; }

        internal bool TriggeredByReplicationThread;

        /// <summary>
        /// Time series document collection name.
        /// </summary>
        public string CollectionName { get; set; }

        public DynamicJsonValue ToJson()
        {
            DateTime? from = null, to = null;

            if (From != DateTime.MinValue)
                from = From;

            if (To != DateTime.MaxValue)
                to = To;

            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(From)] = from,
                [nameof(To)] = to,
                [nameof(DocumentId)] = DocumentId,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(CollectionName)] = CollectionName
            };
        }

        internal static TimeSeriesChange FromJson(BlittableJsonReaderObject value)
        {
            value.TryGet(nameof(Name), out string name);
            value.TryGet(nameof(From), out DateTime? from);
            value.TryGet(nameof(To), out DateTime? to);
            value.TryGet(nameof(DocumentId), out string documentId);
            value.TryGet(nameof(ChangeVector), out string changeVector);
            value.TryGet(nameof(Type), out string type);
            value.TryGet(nameof(CollectionName), out string collectionName);

            return new TimeSeriesChange
            {
                Name = name,
                From = from ?? DateTime.MinValue,
                To = to ?? DateTime.MaxValue,
                DocumentId = documentId,
                ChangeVector = changeVector,
                Type = (TimeSeriesChangeTypes)Enum.Parse(typeof(TimeSeriesChangeTypes), type, ignoreCase: true),
                CollectionName = collectionName 
            };
        }
    }

    public class IndexChange : DatabaseChange
    {
        /// <summary>
        /// Type of change that occurred on index.
        /// </summary>
        public IndexChangeTypes Type { get; set; }

        /// <summary>
        /// Name of index for which notification was created
        /// </summary>
        public string Name { get; set; }

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Name);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Type)] = Type.ToString()
            };
        }

        internal static IndexChange FromJson(BlittableJsonReaderObject value)
        {
            value.TryGet(nameof(Name), out string name);
            value.TryGet(nameof(Type), out string type);

            return new IndexChange
            {
                Type = (IndexChangeTypes)Enum.Parse(typeof(IndexChangeTypes), type, ignoreCase: true),
                Name = name
            };
        }
    }

    public class IndexRenameChange : IndexChange
    {
        /// <summary>
        /// The old index name
        /// </summary>
        public string OldIndexName { get; set; }
    }

    public class TrafficWatchChange : DatabaseChange
    {
        public DateTime TimeStamp { get; set; }
        public int RequestId { get; set; }
        public string HttpMethod { get; set; }
        public long ElapsedMilliseconds { get; set; }
        public int ResponseStatusCode { get; set; }
        public string RequestUri { get; set; }
        public string AbsoluteUri { get; set; }
        public string DatabaseName { get; set; }
        public string CustomInfo { get; set; }
        public TrafficWatchChangeType Type { get; set; }
        public string ClientIP { get; set; }
    }

    public enum TrafficWatchChangeType
    {
        None,
        Queries,
        Operations,
        MultiGet,
        BulkDocs,
        Index,
        Counters,
        Hilo,
        Subscriptions,
        Streams,
        Documents,
        TimeSeries
    }
}
