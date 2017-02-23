//-----------------------------------------------------------------------
// <copyright file="VirtualAttachmentDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;


namespace Raven.Bundles.Replication.Triggers
{
    using Raven.Abstractions.Extensions;

    /// <summary>
    /// We can't allow real deletes when using replication, because
    /// then we won't have any way to replicate the delete. Instead
    /// we allow the delete but don't do actual delete, we replace it 
    /// with a delete marker instead
    /// </summary>
    [ExportMetadata("Bundle", "Replication")]
    [ExportMetadata("Order", 10000)]
    [InheritedExport(typeof(AbstractAttachmentDeleteTrigger))]
    [Obsolete("Use RavenFS instead.")]
    public class VirtualAttachmentDeleteTrigger : AbstractAttachmentDeleteTrigger
    {
        readonly ThreadLocal<RavenJArray> deletedHistory = new ThreadLocal<RavenJArray>();
    
        public override void OnDelete(string key)
        {
            using(Database.DisableAllTriggersForCurrentThread())
            {
                var attachment = Database.Attachments.GetStatic(key);
                if (attachment == null)
                    return;

                if (attachment.IsConflictAttachment() == false && HasConflict(attachment))
                {
                    HandleConflictedAttachment(attachment);
                    return;
                }

                HandleAttachment(attachment);
            }
        }

        public override void AfterDelete(string key)
        {
            var now = DateTime.UtcNow;
            var metadata = new RavenJObject
            {
                {Constants.RavenDeleteMarker, true},
                {Constants.RavenReplicationHistory, deletedHistory.Value},
                {Constants.RavenReplicationSource, Database.TransactionalStorage.Id.ToString()},
                {Constants.RavenReplicationVersion, ReplicationHiLo.NextId(Database)},
                {Constants.LastModified,now},
                {Constants.RavenLastModified,now }
            };
            deletedHistory.Value = null;
            Database.TransactionalStorage.Batch(accessor =>
                accessor.Lists.Set(Constants.RavenReplicationAttachmentsTombstones, key, metadata, UuidType.Attachments));
        
        }

        private void HandleConflictedAttachment(Attachment attachment)
        {
            var attachmentDataStream = attachment.Data();
            var attachmentData = attachmentDataStream.ToJObject();

            var conflicts = attachmentData.Value<RavenJArray>("Conflicts");

            if (conflicts == null)
                return;

            var currentSource = Database.TransactionalStorage.Id.ToString();

            foreach (var c in conflicts)
            {
                var conflict = Database.Attachments.GetStatic(c.Value<string>());
                var conflictSource = conflict.Metadata.Value<RavenJValue>(Constants.RavenReplicationSource).Value<string>();

                if (conflictSource != currentSource)
                    continue;

                this.deletedHistory.Value = new RavenJArray
                {
                    new RavenJObject
                    {
                        { Constants.RavenReplicationVersion, conflict.Metadata[Constants.RavenReplicationVersion] },
                        { Constants.RavenReplicationSource, conflict.Metadata[Constants.RavenReplicationSource] }
                    }
                };

                return;
            }
        }

        private void HandleAttachment(Attachment document)
        {
            deletedHistory.Value = new RavenJArray(ReplicationData.GetOrCreateHistory(document.Metadata))
            {
                new RavenJObject
                {
                    {Constants.RavenReplicationVersion, document.Metadata[Constants.RavenReplicationVersion]},
                    {Constants.RavenReplicationSource, document.Metadata[Constants.RavenReplicationSource]}
                }
            };
        }

        private bool HasConflict(Attachment attachment)
        {
            var conflict = attachment.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);

            return conflict != null && conflict.Value<bool>();
        }

        public override void Dispose()
        {
            this.deletedHistory.Dispose();
            base.Dispose();
        }
    }
}
