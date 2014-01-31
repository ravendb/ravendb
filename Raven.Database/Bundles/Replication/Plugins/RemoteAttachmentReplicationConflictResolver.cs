// -----------------------------------------------------------------------
//  <copyright file="RemoteAttachmentReplicationConflictResolver.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Plugins
{
	[PartNotDiscoverable]
	public class RemoteAttachmentReplicationConflictResolver : AbstractAttachmentReplicationConflictResolver
	{
		public static RemoteAttachmentReplicationConflictResolver Instance = new RemoteAttachmentReplicationConflictResolver();

		public override bool TryResolve(string id, RavenJObject metadata, byte[] data, Attachment existingAttachment, Func<string, Attachment> getAttachment,
		                                out RavenJObject metadataToSave, out byte[] dataToSave)
		{
			metadataToSave = metadata;
			dataToSave = data;

			return true;
		}
	}
}