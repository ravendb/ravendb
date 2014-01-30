// -----------------------------------------------------------------------
//  <copyright file="RemoteDocumentReplicationConflictResolver.cs" company="Hibernating Rhinos LTD">
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
	public class RemoteDocumentReplicationConflictResolver : AbstractDocumentReplicationConflictResolver
	{
		public static RemoteDocumentReplicationConflictResolver Instance = new RemoteDocumentReplicationConflictResolver();

		public override bool TryResolve(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc,
		                                Func<string, JsonDocument> getDocument, out RavenJObject metadataToSave,
		                                out RavenJObject documentToSave)
		{
			metadataToSave = metadata;
			documentToSave = document;

			return true;
		}
	}
}