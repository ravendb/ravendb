//-----------------------------------------------------------------------
// <copyright file="AncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json.Linq;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Triggers
{
    [ExportMetadata("Bundle", "Replication")]
    [ExportMetadata("Order", 10000)]
    [InheritedExport(typeof(AbstractPutTrigger))]
    public class AncestryPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, RavenJObject jsonReplicationDocument, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) && // we don't deal with system documents
                key.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase) == false) // except for hilos
                return;

            using (Database.DisableAllTriggersForCurrentThread())
            {
                var documentMetadata = GetDocumentMetadata(key);
                if (documentMetadata != null)
                {
                    var history = new RavenJArray(ReplicationData.GetHistory(documentMetadata));

                    if (documentMetadata.ContainsKey(Constants.RavenReplicationMergedHistory) == false)
                    {
                        if (documentMetadata.ContainsKey(Constants.RavenReplicationVersion) &&
                            documentMetadata.ContainsKey(Constants.RavenReplicationSource))
                        {
                            history.Add(new RavenJObject
                            {
                                {Constants.RavenReplicationVersion, documentMetadata[Constants.RavenReplicationVersion]},
                                {Constants.RavenReplicationSource, documentMetadata[Constants.RavenReplicationSource]}
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
                            if (RavenJTokenEqualityComparer.Default.Equals(((RavenJObject) currentEntry)
                                [Constants.RavenReplicationSource], documentMetadata[Constants.RavenReplicationSource]))
                                break;
                        }
                        if(i!=-1)
                            history.RemoveAt(i);
                        history.Add(new RavenJObject
                            {
                                {Constants.RavenReplicationVersion, documentMetadata[Constants.RavenReplicationVersion]},
                                {Constants.RavenReplicationSource, documentMetadata[Constants.RavenReplicationSource]}
                            });
                        metadata[Constants.RavenReplicationHistory] = history;
                    }
                    
                }

                metadata[Constants.RavenReplicationVersion] = RavenJToken.FromObject(ReplicationHiLo.NextId(Database));
                metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(Database.TransactionalStorage.Id);
            }
        }

        private RavenJObject GetDocumentMetadata(string key)
        {
            var doc = Database.Documents.GetDocumentMetadata(key, null);
            if (doc != null)
            {
                var doesNotExist = doc.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists); // occurs when in transaction

                if (doesNotExist == false)
                    return doc.Metadata;
            }

            RavenJObject result = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                var tombstone = accessor.Lists.Read(Constants.RavenReplicationDocsTombstones, key);
                if (tombstone == null)
                    return;
                result = tombstone.Data;
                accessor.Lists.Remove(Constants.RavenReplicationDocsTombstones, key);
            });
            return result;
        }

        public override IEnumerable<string> GeneratedMetadataNames
        {
            get
            {
                return new[]
                {
                    Constants.RavenReplicationVersion,
                    Constants.RavenReplicationSource
                };
            }
        }
    }
}
