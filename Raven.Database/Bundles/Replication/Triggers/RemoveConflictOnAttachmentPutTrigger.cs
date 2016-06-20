//-----------------------------------------------------------------------
// <copyright file="RemoveConflictOnAttachmentPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json.Linq;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Triggers
{
    [ExportMetadata("Bundle", "Replication")]
    [ExportMetadata("Order", 10000)]
    [InheritedExport(typeof(AbstractAttachmentPutTrigger))]
    [Obsolete("Use RavenFS instead.")]
    public class RemoveConflictOnAttachmentPutTrigger : AbstractAttachmentPutTrigger
    {
        public override void OnPut(string key, Stream data, RavenJObject metadata)
        {
            using (Database.DisableAllTriggersForCurrentThread())
            {
                metadata.Remove(Constants.RavenReplicationConflict);// you can't put conflicts
                metadata.Remove(Constants.RavenReplicationConflictDocument);

                var oldVersion = Database.Attachments.GetStatic(key);
                if (oldVersion == null)
                    return;
                if (oldVersion.Metadata[Constants.RavenReplicationConflict] == null)
                    return;

                var history = new RavenJArray(ReplicationData.GetHistory(metadata));
                metadata[Constants.RavenReplicationHistory] = history;

                // this is a conflict document, holding document keys in the 
                // values of the properties
                var conflictData = oldVersion.Data().ToJObject();
                var conflicts = conflictData.Value<RavenJArray>("Conflicts");
                if (conflicts == null)
                    return;
                foreach (var prop in conflicts)
                {
                    var id = prop.Value<string>();
                    Attachment attachment = Database.Attachments.GetStatic(id);
                    if(attachment == null)
                        continue;
                    Database.Attachments.DeleteStatic(id, null);

                    // add the conflict history to the mix, so we make sure that we mark that we resolved the conflict
                    var conflictHistory = new RavenJArray(ReplicationData.GetHistory(attachment.Metadata));
                    conflictHistory.Add(new RavenJObject
                    {
                        {Constants.RavenReplicationVersion, attachment.Metadata[Constants.RavenReplicationVersion]},
                        {Constants.RavenReplicationSource, attachment.Metadata[Constants.RavenReplicationSource]}
                    });

                    foreach (var item in conflictHistory)
                    {
                        if (history.Any(x => RavenJTokenEqualityComparer.Default.Equals(x, item)))
                            continue;
                        history.Add(item);
                    }
                }
            }
        }
    }
}
