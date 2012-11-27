// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class DocumentChangeNotification : EventArgs
	{
		public DocumentChangeTypes Type { get; set; }
		public string Id { get; set; }
		public Guid? Etag { get; set; }

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
		ReplicationConflict = 4,
		AttachmentReplicationConflict = 8,

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
		IndexRemoved = 16
	}

	public class IndexChangeNotification : EventArgs
	{
		public IndexChangeTypes Type { get; set; }
		public string Name { get; set; }
		public Guid? Etag { get; set; }

		public override string ToString()
		{
			return string.Format("{0} on {1}", Type, Name);
		}
	}
}