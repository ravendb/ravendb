//-----------------------------------------------------------------------
// <copyright file="VirtualDeleteAndRemoveConflictsTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Impl;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Abstractions.Threading;

namespace Raven.Bundles.Replication.Triggers
{
    /// <summary>
    /// We can't allow real deletes when using replication, because
    /// then we won't have any way to replicate the delete. Instead
    /// we allow the delete but don't do actual delete, we replace it 
    /// with a delete marker instead
    /// </summary>
    [ExportMetadata("Bundle", "Replication")]
    [ExportMetadata("Order", 10000)]
    [InheritedExport(typeof(AbstractDeleteTrigger))]
    public class VirtualDeleteAndRemoveConflictsTrigger : AbstractDeleteTrigger
    {
        readonly Raven.Abstractions.Threading.ThreadLocal<RavenJArray> deletedHistory = new Raven.Abstractions.Threading.ThreadLocal<RavenJArray>();

        public override void OnDelete(string key, TransactionInformation transactionInformation)
        {
            using (Database.DisableAllTriggersForCurrentThread())
            {
                var metadata = Database.Documents.GetDocumentMetadata(key, transactionInformation);

                if (metadata == null)
                    return;

                JsonDocument document = null;
                if (IsConflictDocument(metadata, transactionInformation, ref document) == false && HasConflict(metadata, transactionInformation, ref document))
                {
                    HandleConflictedDocument(document, transactionInformation);
                    return;
                }

                HandleDocument(metadata);
            }
        }

        public override void AfterDelete(string key, TransactionInformation transactionInformation,RavenJObject metaJObject)
        {
            var now = DateTime.UtcNow;
            var metadata = new RavenJObject
            {
                {Constants.RavenDeleteMarker, true},
                {Constants.RavenReplicationHistory, deletedHistory.Value},
                {Constants.RavenReplicationSource, Database.TransactionalStorage.Id.ToString()},
                {Constants.RavenReplicationVersion, ReplicationHiLo.NextId(Database)},
                {Constants.LastModified, now},
                {Constants.RavenLastModified, now}
            };
            //Adding Raven-Entity-Name to tombstones so we can filter them when using ETL.
            if (metaJObject.ContainsKey(Constants.RavenEntityName))
                metadata[Constants.RavenEntityName] = metaJObject[Constants.RavenEntityName];
            deletedHistory.Value = null;

            Database.TransactionalStorage.Batch(accessor =>
                accessor.Lists.Set(Constants.RavenReplicationDocsTombstones, key, metadata, UuidType.Documents));
        }

        private void HandleConflictedDocument(JsonDocument document, TransactionInformation transactionInformation)
        {
            var conflicts = document.DataAsJson.Value<RavenJArray>("Conflicts");
            var currentSource = Database.TransactionalStorage.Id.ToString();
            var historySet = false;

            foreach (var c in conflicts)
            {
                RavenJObject conflict;
                if (Database.Documents.Delete(c.Value<string>(), null, transactionInformation, out conflict) == false)
                    continue;

                if (historySet)
                    continue;

                var conflictSource = conflict.Value<RavenJValue>(Constants.RavenReplicationSource).Value<string>();

                if (conflictSource != currentSource)
                    continue;

                deletedHistory.Value = new RavenJArray
                {
                    new RavenJObject
                    {
                        { Constants.RavenReplicationVersion, conflict[Constants.RavenReplicationVersion] },
                        { Constants.RavenReplicationSource, conflict[Constants.RavenReplicationSource] }
                    }
                };

                historySet = true;
            }
        }

        private void HandleDocument(JsonDocumentMetadata metadata)
        {
            deletedHistory.Value = new RavenJArray(ReplicationData.GetHistory(metadata.Metadata))
            {
                new RavenJObject
                {
                    {Constants.RavenReplicationVersion, metadata.Metadata[Constants.RavenReplicationVersion]},
                    {Constants.RavenReplicationSource, metadata.Metadata[Constants.RavenReplicationSource]}
                }
            };
        }

        private bool HasConflict(JsonDocumentMetadata metadata, TransactionInformation transactionInformation, ref JsonDocument document)
        {
            var conflict = metadata.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);

            if (conflict != null && conflict.Value<bool>())
            {
                document = document ?? Database.Documents.Get(metadata.Key, transactionInformation);
                return document.DataAsJson.Value<RavenJArray>("Conflicts") != null;
            }

            return false;
        }

        public bool IsConflictDocument(JsonDocumentMetadata metadata, TransactionInformation transactionInformation, ref JsonDocument document)
        {
            var conflict = metadata.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);
            if (conflict == null || conflict.Value<bool>() == false)
            {
                return false;
            }

            var keyParts = metadata.Key.Split('/');
            if (keyParts.Contains("conflicts") == false)
            {
                return false;
            }

            document = Database.Documents.Get(metadata.Key, transactionInformation);

            var conflicts = document.DataAsJson.Value<RavenJArray>("Conflicts");
            if (conflicts != null)
            {
                return false;
            }

            return true;
        }
    }
}