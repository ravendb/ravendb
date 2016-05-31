// -----------------------------------------------------------------------
//  <copyright file="LocalDocumentReplicationConflictResolver.cs" company="Hibernating Rhinos LTD">
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
    public class LocalDocumentReplicationConflictResolver : AbstractDocumentReplicationConflictResolver
    {
        public static LocalDocumentReplicationConflictResolver Instance = new LocalDocumentReplicationConflictResolver();

        protected override bool TryResolve(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc,
                                        Func<string, JsonDocument> getDocument, out RavenJObject metadataToSave,
                                        out RavenJObject documentToSave)
        {
            // we can't resolve to local when local is conflicted
            if (existingDoc.Metadata.Value<bool>(Constants.RavenReplicationConflict))
            {
                documentToSave = null;
                metadataToSave = null;
                return false;
            }

            metadataToSave = existingDoc.Metadata;
            documentToSave = existingDoc.DataAsJson;

            return true;
        }
    }
}
