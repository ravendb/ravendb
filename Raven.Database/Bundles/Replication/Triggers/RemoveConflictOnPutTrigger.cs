//-----------------------------------------------------------------------
// <copyright file="RemoveConflictOnPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Triggers
{
    [ExportMetadata("Bundle", "Replication")]
    [ExportMetadata("Order", 10000)]
    [InheritedExport(typeof(AbstractPutTrigger))]
    public class RemoveConflictOnPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, RavenJObject jsonReplicationDocument, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            using (Database.DisableAllTriggersForCurrentThread())
            {
                if (metadata.Remove(Constants.RavenReplicationConflictSkipResolution))
                {
                    if (key.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase) == -1)
                    {
                        metadata["@Http-Status-Code"] = 409;
                        metadata["@Http-Status-Description"] = "Conflict";
                    }

                    return;
                }

                metadata.Remove(Constants.RavenReplicationConflict);// you can't put conflicts
                metadata.Remove(Constants.RavenReplicationConflictDocument);

                var oldVersion = Database.Documents.Get(key, transactionInformation);
                if (oldVersion == null)
                    return;
                if (oldVersion.Metadata[Constants.RavenReplicationConflict] == null)
                    return;

                var history = new RavenJArray();
                metadata[Constants.RavenReplicationHistory] = history;

                // this is a conflict document, holding document keys in the 
                // values of the properties
                var conflicts = oldVersion.DataAsJson.Value<RavenJArray>("Conflicts");
                if(conflicts == null)
                    return;

                var list = new List<RavenJArray>
                {
                    new RavenJArray(ReplicationData.GetHistory(metadata)) // first item to interleave
                };

                foreach (var prop in conflicts)
                {
                    RavenJObject deletedMetadata;
                    Database.Documents.Delete(prop.Value<string>(), null, transactionInformation, out deletedMetadata);

                    if (deletedMetadata != null)
                    {
                        var conflictHistory = new RavenJArray(ReplicationData.GetHistory(deletedMetadata));
                        conflictHistory.Add(new RavenJObject
                        {
                            {Constants.RavenReplicationVersion, deletedMetadata[Constants.RavenReplicationVersion]},
                            {Constants.RavenReplicationSource, deletedMetadata[Constants.RavenReplicationSource]}
                        });
                        list.Add(conflictHistory);
                    }
                }


                int index = 0;
                bool added = true;
                while (added) // interleave the history from all conflicts
                {
                    added = false;
                    foreach (var deletedMetadata in list)
                    {
                        // add the conflict history to the mix, so we make sure that we mark that we resolved the conflict
                        if (index < deletedMetadata.Length)
                        {
                            history.Add(deletedMetadata[index]);
                            added = true;
                        }
                    }
                    index++;
                }

                while (history.Length > Constants.ChangeHistoryLength)
                {
                    history.RemoveAt(0);
                }
            }
        }

        public override IEnumerable<string> GeneratedMetadataNames
        {
            get
            {
                return new[]
                {
                    Constants.RavenReplicationHistory
                };
            }
        }
    }
}
