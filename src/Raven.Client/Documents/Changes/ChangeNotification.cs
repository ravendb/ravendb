// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.ServerWide.Tcp;
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
        /// Document type name.
        /// </summary>
        [Obsolete("DatabaseChanges.ForDocumentsOfType is not supported anymore. Will be removed in next major version of the product.")]
        public string TypeName { get; set; }

        /// <summary>
        /// Document change vector
        /// </summary>
        public string ChangeVector { get; set; }

        internal bool TriggeredByReplicationThread;

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Id);
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

        IndexAdded = 8,
        IndexRemoved = 16,

        IndexDemotedToIdle = 32,
        IndexPromotedFromIdle = 64,

        IndexDemotedToDisabled = 256,

        IndexMarkedAsErrored = 512,

        SideBySideReplace = 1024,

        Renamed = 2048,
        IndexPaused = 4096,
        LockModeChanged = 8192,
        PriorityChanged = 16384
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
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString()
            };
        }

        internal static CounterChange FromJson(BlittableJsonReaderObject value)
        {
            value.TryGet(nameof(Name), out string name);
            value.TryGet(nameof(Value), out long val);
            value.TryGet(nameof(DocumentId), out string documentId);
            value.TryGet(nameof(ChangeVector), out string changeVector);
            value.TryGet(nameof(Type), out string type);

            return new CounterChange
            {
                Name = name,
                Value = val,
                DocumentId = documentId,
                ChangeVector = changeVector,
                Type = (CounterChangeTypes)Enum.Parse(typeof(CounterChangeTypes), type, ignoreCase: true)
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
    internal abstract class TrafficWatchChangeBase : DatabaseChange, IDynamicJson
    {
        public abstract TrafficWatchType TrafficWatchType { get; }
        public DateTime TimeStamp { get; set; }
        public string DatabaseName { get; set; }
        public string CustomInfo { get; set; }
        public string ClientIP { get; set; }
        public string CertificateThumbprint { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(TrafficWatchType)] = TrafficWatchType,
                [nameof(TimeStamp)] = TimeStamp,
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(CustomInfo)] = CustomInfo,
                [nameof(ClientIP)] = ClientIP,
                [nameof(CertificateThumbprint)] = CertificateThumbprint
            };

            return json;
        }
    }

    public enum TrafficWatchType
    {
        Http,
        Tcp
    }

    internal class TrafficWatchHttpChange : TrafficWatchChangeBase
    {
        public override TrafficWatchType TrafficWatchType => TrafficWatchType.Http;
        public int RequestId { get; set; }
        public string HttpMethod { get; set; }
        public long ElapsedMilliseconds { get; set; }
        public int ResponseStatusCode { get; set; }
        public string RequestUri { get; set; }
        public string AbsoluteUri { get; set; }
        public TrafficWatchChangeType Type { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RequestId)] = RequestId;
            json[nameof(HttpMethod)] = HttpMethod;
            json[nameof(ElapsedMilliseconds)] = ElapsedMilliseconds;
            json[nameof(ResponseStatusCode)] = ResponseStatusCode;
            json[nameof(RequestUri)] = RequestUri;
            json[nameof(AbsoluteUri)] = AbsoluteUri;
            json[nameof(Type)] = Type;
            return json;
        }
    }

    internal class TrafficWatchTcpChange : TrafficWatchChangeBase
    {
        public override TrafficWatchType TrafficWatchType => TrafficWatchType.Tcp;
        public string Source { get; set; }
        public TcpConnectionHeaderMessage.OperationTypes Operation { get; set; }
        public int OperationVersion { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Source)] = Source;
            json[nameof(Operation)] = Operation;
            json[nameof(OperationVersion)] = OperationVersion;
            return json;
        }
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
        Documents
    }
}
