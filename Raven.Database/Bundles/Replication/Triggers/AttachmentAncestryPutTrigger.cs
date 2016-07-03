//-----------------------------------------------------------------------
// <copyright file="AttachmentAncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json.Linq;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Triggers
{
    [ExportMetadata("Bundle", "Replication")]
    [ExportMetadata("Order", 10000)]
    [InheritedExport(typeof(AbstractAttachmentPutTrigger))]
    [Obsolete("Use RavenFS instead.")]
    public class AttachmentAncestryPutTrigger : AbstractAttachmentPutTrigger
    {

        public override void OnPut(string key, Stream data, RavenJObject metadata)
        {
            if (key.StartsWith("Raven/")) // we don't deal with system attachment
                return;

            using (Database.DisableAllTriggersForCurrentThread())
            {
                var attachmentMetadata = GetAttachmentMetadata(key);
                if (attachmentMetadata != null)
                {
                    var history = new RavenJArray(ReplicationData.GetHistory(attachmentMetadata));

                    if (attachmentMetadata.ContainsKey(Constants.RavenReplicationMergedHistory) == false)
                    {
                        if (attachmentMetadata.ContainsKey(Constants.RavenReplicationVersion) &&
                            attachmentMetadata.ContainsKey(Constants.RavenReplicationSource))
                        {
                            history.Add(new RavenJObject
                            {
                                {Constants.RavenReplicationVersion, attachmentMetadata[Constants.RavenReplicationVersion]},
                                {Constants.RavenReplicationSource, attachmentMetadata[Constants.RavenReplicationSource]}
                            });
                        }
                        else
                        {
                            history.Add(new RavenJObject
                            {
                                {Constants.RavenReplicationVersion, 0},
                                {Constants.RavenReplicationSource, RavenJToken.FromObject(Database.TransactionalStorage.Id)}
                            });
                        }

                        var sources = new HashSet<RavenJToken>(RavenJTokenEqualityComparer.Default);
                        int pos = history.Length - 1;
                        for (; pos >= 0; pos--)
                        {
                            var source = ((RavenJObject)history[pos])[Constants.RavenReplicationSource];
                            if (sources.Contains(source))
                            {
                                history.RemoveAt(pos);
                                continue;
                            }
                            sources.Add(source);

                        }
                        metadata[Constants.RavenReplicationMergedHistory] = true;
                        metadata[Constants.RavenReplicationHistory] = history;
                    }
                    //If we have the flag we must have Constants.RavenReplicationVersion and Constants.RavenReplicationSource too
                    //Here we assume that the replication history is in the form of a "sorted dictionary" so we just need to remove
                    //the entry with the current source id and insert the new version at the end of the history.
                    else
                    {
                        int i = history.Length - 1;
                        for (; i >= 0; i--)
                        {
                            var currentEntry = history[i];
                            if (RavenJTokenEqualityComparer.Default.Equals(((RavenJObject)currentEntry)
                                [Constants.RavenReplicationSource], attachmentMetadata[Constants.RavenReplicationSource]))
                                break;
                        }
                        if (i != -1)
                            history.RemoveAt(i);
                        history.Add(new RavenJObject
                            {
                                {Constants.RavenReplicationVersion, attachmentMetadata[Constants.RavenReplicationVersion]},
                                {Constants.RavenReplicationSource, attachmentMetadata[Constants.RavenReplicationSource]}
                            });
                        metadata[Constants.RavenReplicationHistory] = history;
                    }
                }

                metadata[Constants.RavenReplicationVersion] = RavenJToken.FromObject(ReplicationHiLo.NextId(Database));
                metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
            }
        }

        private RavenJObject GetAttachmentMetadata(string key)
        {
            var attachment = Database.Attachments.GetStatic(key);
            if(attachment != null)
                return attachment.Metadata;

            RavenJObject result = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                var tombstone = accessor.Lists.Read(Constants.RavenReplicationAttachmentsTombstones, key);
                if (tombstone == null)
                    return;
                result = tombstone.Data;
                accessor.Lists.Remove(Constants.RavenReplicationAttachmentsTombstones, key);
            });

            return result;
        }
    }
}
