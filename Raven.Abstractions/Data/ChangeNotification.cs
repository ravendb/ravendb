// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class BulkInsertChangeNotification : DocumentChangeNotification
	{
		public Guid OperationId { get; set; }
	}

	public class DocumentChangeNotification : EventArgs
	{
		public DocumentChangeTypes Type { get; set; }
		public string Id { get; set; }
		public string CollectionName { get; set; }
		public string TypeName { get; set; }
		public Etag Etag { get; set; }
		public string Message { get; set; }

		public override string ToString()
		{
			return string.Format("{0} on {1}", Type, Id);
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

		MapCompleted = 1,
		ReduceCompleted = 2,
		RemoveFromIndex = 4,

		IndexAdded = 8,
		IndexRemoved = 16,

        IndexDemotedToIdle = 32,
        IndexPromotedFromIdle = 64,

		IndexDemotedToAbandoned = 128,

		IndexDemotedToDisabled = 256,

        IndexMarkedAsErrored = 512
	}

	public class IndexChangeNotification : EventArgs
	{
		public IndexChangeTypes Type { get; set; }
		public string Name { get; set; }
		public Etag Etag { get; set; }

		public override string ToString()
		{
			return string.Format("{0} on {1}", Type, Name);
		}
	}

	public class ReplicationConflictNotification : EventArgs
	{
		public ReplicationConflictTypes ItemType { get; set; }
		public string Id { get; set; }
		public Etag Etag { get; set; }
		public ReplicationOperationTypes OperationType { get; set; }
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
		AttachmentReplicationConflict = 2,
	}

	[Flags]
	public enum ReplicationOperationTypes
	{
		None = 0,

		Put = 1,
		Delete = 2,
	}
}