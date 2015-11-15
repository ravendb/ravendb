// -----------------------------------------------------------------------
//  <copyright file="LocalDocumentReplicationConflictResolver.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Plugins
{
    [PartNotDiscoverable]
    [Obsolete("Use RavenFS instead.")]
    public class LocalAttachmentReplicationConflictResolver : AbstractAttachmentReplicationConflictResolver
    {
        public static LocalAttachmentReplicationConflictResolver Instance = new LocalAttachmentReplicationConflictResolver();

        protected override bool TryResolve(string id, RavenJObject metadata, byte[] data, Attachment existingAttachment, Func<string, Attachment> getAttachment, out RavenJObject metadataToSave,
                                        out byte[] dataToSave)
        {
            metadataToSave = existingAttachment.Metadata;
            dataToSave = existingAttachment.Data().ReadData();

            return true;
        }
    }
}
