// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class ChangeNotification : EventArgs
	{
		public ChangeTypes Type { get; set; }
		public string Name { get; set; }
		public Guid? Etag { get; set; }

		public override string ToString()
		{
			return string.Format("{0} on {1}", Type, Name);
		}
	}

	[Flags]
	public enum ChangeTypes
	{
		None = 0,

		Put = 1,
		Delete = 2,
		IndexUpdated = 4,
		ReplicationConflict = 8,
		AttachmentReplicationConflict = 16,

		Common = Put | Delete | IndexUpdated
	}
}