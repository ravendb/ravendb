// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Client.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public class BulkInsertChangeNotification : DocumentChangeNotification
    {
        /// <summary>
        /// BulkInsert operation Id.
        /// </summary>
        public Guid OperationId { get; set; }
    }

    public class DocumentChangeNotification : Notification
    {
        /// <summary>
        /// Type of change that occurred on document.
        /// </summary>
        public DocumentChangeTypes Type { get; set; }

        /// <summary>
        /// Identifier of document for which notification was created.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Document collection name.
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// Document type name.
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Document etag.
        /// </summary>
        public long? Etag { get; set; }

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

        IndexDemotedToAbandoned = 128,

        IndexDemotedToDisabled = 256,

        IndexMarkedAsErrored = 512,

        SideBySideReplace = 1024
    }

    public enum TransformerChangeTypes
    {
        None = 0,

        TransformerAdded = 1,
        TransformerRemoved = 2
    }

    public class IndexChangeNotification : Notification
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
        /// The index version that changed
        /// </summary>
        public int? Version { get; set; }
        /// <summary>
        /// TODO [ppekrol]
        /// </summary>
        public long? Etag { get; set; }

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Name);
        }
    }

    public class TransformerChangeNotification : Notification
    {
        /// <summary>
        /// Type of change that occurred on transformer.
        /// </summary>
        public TransformerChangeTypes Type { get; set; }

        /// <summary>
        /// Name of transformer for which notification was created
        /// </summary>
        public string Name { get; set; }

        public override string ToString()
        {
            return string.Format("{0} on {1}", Type, Name);
        }
    }

    public class ReplicationConflictNotification : Notification
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

    public class TrafficWatchNotification : Notification
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
        public RavenJObject QueryTimings { get; set; }
    }

    public class DataSubscriptionChangeNotification : EventArgs
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
