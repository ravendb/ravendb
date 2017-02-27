// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Newtonsoft.Json;

namespace Raven.Client.Documents.Changes
{
    public class BulkInsertChange : DocumentChange
    {
        /// <summary>
        /// BulkInsert operation Id.
        /// </summary>
        public Guid OperationId { get; set; }
    }

    public class AttachmentChange : DocumentChange
    {
        /// <summary>
        /// The attachment name.
        /// </summary>
        public string Name { get; set; }
    }

    public class DocumentChange : DatabaseChange
    {
        private string _key;

        [JsonIgnore]
        public Func<object, string> MaterializeKey;

        public object MaterializeKeyState;

        /// <summary>
        /// Type of change that occurred on document.
        /// </summary>
        public DocumentChangeTypes Type { get; set; }

        /// <summary>
        /// Identifier of document for which notification was created.
        /// </summary>
        public string Key
        {
            get
            {
                if (_key == null && MaterializeKey != null)
                {
                    _key = MaterializeKey(MaterializeKeyState);
                    MaterializeKey = null;
                    MaterializeKeyState = null;
                }
                return _key;
            }
            set { _key = value; }
        }

        /// <summary>
        /// Document collection name.
        /// </summary>
        public string CollectionName { get; set; }

        public bool IsSystemDocument { get; set; }

        /// <summary>
        /// Document type name.
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Document etag.
        /// </summary>
        public long? Etag { get; set; }

        internal bool TriggeredByReplicationThread;

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Key);
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
        Common = Put | Delete,
        PutAttachment = 128,
        DeleteAttachment = 256,
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

        Renamed = 2048
    }

    public enum TransformerChangeTypes
    {
        None = 0,

        TransformerAdded = 1,
        TransformerRemoved = 2,
        TransformerRenamed = 4
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

        /// <summary>
        /// The index etag
        /// </summary>
        public long? Etag { get; set; }

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Name);
        }
    }

    public class TransformerChange : DatabaseChange
    {
        /// <summary>
        /// Type of change that occurred on transformer.
        /// </summary>
        public TransformerChangeTypes Type { get; set; }

        /// <summary>
        /// Name of transformer for which notification was created
        /// </summary>
        public string Name { get; set; }

        public long Etag { get; set; }

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Name);
        }
    }

    public class ReplicationConflictChange : DatabaseChange
    {
        /// <summary>
        /// Type of conflict that occurred (None, DocumentReplicationConflict).
        /// </summary>
        public ReplicationConflictTypes ItemType { get; set; }

        /// <summary>
        /// Identifier of a document on which replication conflict occurred.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Current conflict document Etag.
        /// </summary>
        public long? Etag { get; set; }

        /// <summary>
        /// Operation type on which conflict occurred (Put, Delete).
        /// </summary>
        public ReplicationOperationTypes OperationType { get; set; }

        /// <summary>
        /// Array of conflict document Ids.
        /// </summary>
        public string[] Conflicts { get; set; }

        public override string ToString()
        {
            return string.Format("{0} on {1} because of {2} operation", ItemType, Id, OperationType);
        }
    }

    [Flags]
    public enum ReplicationConflictTypes
    {
        None = 0,

        DocumentReplicationConflict = 1,
    }

    [Flags]
    public enum ReplicationOperationTypes
    {
        None = 0,

        Put = 1,
        Delete = 2,
    }

    internal class TrafficWatchChange : DatabaseChange
    {
        public DateTime TimeStamp { get; set; }
        public int RequestId { get; set; }
        public string HttpMethod { get; set; }
        public long ElapsedMilliseconds { get; set; }
        public int ResponseStatusCode { get; set; }
        public string RequestUri { get; set; }
        public string AbsoluteUri { get; set; }
        public string TenantName { get; set; }
        public string CustomInfo { get; set; }
        public int InnerRequestsCount { get; set; }
        public object QueryTimings { get; set; } // TODO: fix this
    }

    public class DataSubscriptionChange : EventArgs
    {
        /// <summary>
        /// Subscription identifier for which a notification was created
        /// </summary>
        public long Id { get; set; }

        /// <summary>
        /// Type of subscription change
        /// </summary>
        public DataSubscriptionChangeTypes Type { get; set; }
    }

    public enum DataSubscriptionChangeTypes
    {
        None = 0,

        SubscriptionOpened = 1,
        SubscriptionReleased = 2
    }
}
