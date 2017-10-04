//-----------------------------------------------------------------------
// <copyright file="VersioningPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Triggers;
using Raven.Bundles.Versioning.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Versioning.Triggers
{
    [InheritedExport(typeof(AbstractPutTrigger))]
    [ExportMetadata("Bundle", "Versioning")]
    public class VersioningPutTrigger : AbstractPutTrigger
    {
        internal const string CreationOfHistoricalRevisionIsNotAllowed = "Creating a historical revision is not allowed";
        internal const string ModificationOfHistoricalRevisionIsNotAllowed = "Modifying a historical revision is not allowed";

        public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (Database.IsVersioningActive(metadata) == false)
                return VetoResult.Allowed;

            if (Database.IsVersioningDisabledForImport(metadata))
                return VetoResult.Allowed;

            var jsonDocument = Database.Documents.Get(key, transactionInformation);

            if (Database.ChangesToRevisionsAllowed() == false &&
                (jsonDocument?.Metadata ?? metadata).Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical")
            {
                return VetoResult.Deny(jsonDocument == null ? CreationOfHistoricalRevisionIsNotAllowed : ModificationOfHistoricalRevisionIsNotAllowed);
            }

            return VetoResult.Allowed;
        }

        public override void OnPut(string key, RavenJObject jsonReplicationDocument, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            VersioningConfiguration versioningConfiguration;

            if (metadata.ContainsKey(Constants.RavenCreateVersion))
            {
                metadata.__ExternalState[Constants.RavenCreateVersion] = metadata[Constants.RavenCreateVersion];
                metadata.Remove(Constants.RavenCreateVersion);
            }

            if (metadata.ContainsKey(Constants.RavenIgnoreVersioning))
            {
                metadata.__ExternalState[Constants.RavenIgnoreVersioning] = metadata[Constants.RavenIgnoreVersioning];
                metadata.Remove(Constants.RavenIgnoreVersioning);
                return;
            }

            if (TryGetVersioningConfiguration(key, metadata, out versioningConfiguration) == false)
                return;

            var revision = GetNextRevisionNumber(key);

            using (Database.DisableAllTriggersForCurrentThread(new HashSet<Type>{ typeof(VirtualDeleteAndRemoveConflictsTrigger) }))
            {
                RemoveOldRevisions(key, revision, versioningConfiguration, transactionInformation);
            }
            metadata.__ExternalState["Next-Revision"] = revision;

            metadata.__ExternalState["Parent-Revision"] = metadata.Value<string>(VersioningUtil.RavenDocumentRevision);

            metadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Current");
            metadata[VersioningUtil.RavenDocumentRevision] = RavenJToken.FromObject(revision);
        }

        public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, Etag etag, TransactionInformation transactionInformation)
        {
            VersioningConfiguration versioningConfiguration;
            if (TryGetVersioningConfiguration(key, metadata, out versioningConfiguration) == false)
                return;

            using (Database.DisableAllTriggersForCurrentThread())
            {
                var copyMetadata = new RavenJObject(metadata);
                copyMetadata[VersioningUtil.RavenDocumentRevisionStatus] = RavenJToken.FromObject("Historical");
                copyMetadata[Constants.RavenReadOnly] = true;
                copyMetadata.Remove(VersioningUtil.RavenDocumentRevision);
                object parentRevision;
                metadata.__ExternalState.TryGetValue("Parent-Revision", out parentRevision);
                if (parentRevision != null)
                {
                    copyMetadata[VersioningUtil.RavenDocumentParentRevision] = key + "/revisions/" + parentRevision;
                }

                object value;
                metadata.__ExternalState.TryGetValue("Next-Revision", out value);
                Database.Documents.Put(key + "/revisions/" + value, null, (RavenJObject)document.CreateSnapshot(), copyMetadata,
                             transactionInformation);
            }
        }

        private long GetNextRevisionNumber(string key)
        {
            long revision = 0;

            Database.TransactionalStorage.Batch(accessor =>
            {
                revision = Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key + "/revisions/", accessor);

                if (revision == 1)
                {
                    var existingDoc = Database.Documents.Get(key, null);
                    if (existingDoc != null)
                    {
                        RavenJToken existingRevisionToken;
                        if (existingDoc.Metadata.TryGetValue(VersioningUtil.RavenDocumentRevision, out existingRevisionToken))
                            revision = existingRevisionToken.Value<int>() + 1;
                    }
                    else
                    {
                        var latestRevisionsDoc = GetLatestRevisionsDoc(key);
                        if (latestRevisionsDoc != null)
                        {
                            var id = latestRevisionsDoc["@metadata"].Value<string>("@id");
                            if(id.StartsWith(key, StringComparison.CurrentCultureIgnoreCase))
                            {
                                var revisionNum = id.Substring((key + "/revisions/").Length);
                                int result;
                                if (int.TryParse(revisionNum, out result))
                                    revision = result + 1;
                            }
                            
                        }
                    }

                    if (revision > 1)
                        accessor.General.SetIdentityValue(key + "/revisions/", revision);
                }
            });

            return revision;
        }

        private RavenJObject GetLatestRevisionsDoc(string key)
        {
            const int pageSize = 100;
            int start = 0;

            RavenJObject lastRevisionDoc = null;

            while (true)
            {
                int nextPageStart = start; // will trigger rapid pagination
                var docs = Database.Documents.GetDocumentsWithIdStartingWith(key + "/revisions/", null, null, start, pageSize, CancellationToken.None, ref nextPageStart);
                if (!docs.Any())
                    break;

                lastRevisionDoc = (RavenJObject)docs.Last();

                if (docs.Length < pageSize)
                    break;

                start += pageSize;
            }

            return lastRevisionDoc;
        }

        private bool TryGetVersioningConfiguration(string key, RavenJObject metadata,
                                                   out VersioningConfiguration versioningConfiguration)
        {
            versioningConfiguration = null;
            if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
                return false;

            if (metadata.Value<string>(VersioningUtil.RavenDocumentRevisionStatus) == "Historical")
                return false;

            versioningConfiguration = Database.GetDocumentVersioningConfiguration(metadata);
            if (versioningConfiguration == null || versioningConfiguration.Exclude
                || (versioningConfiguration.ExcludeUnlessExplicit && !metadata.__ExternalState.ContainsKey(Constants.RavenCreateVersion))
                || metadata.__ExternalState.ContainsKey(Constants.RavenIgnoreVersioning))
                return false;
            return true;
        }

        private void RemoveOldRevisions(string key, long revision, VersioningConfiguration versioningConfiguration, TransactionInformation transactionInformation)
        {
            long latestValidRevision = revision - versioningConfiguration.MaxRevisions;
            if (latestValidRevision <= 0)
                return;

            Database.Documents.Delete(string.Format("{0}/revisions/{1}", key, latestValidRevision), null, transactionInformation);
        }

        public override IEnumerable<string> GeneratedMetadataNames
        {
            get
            {
                return new[]
                {
                    VersioningUtil.RavenDocumentRevisionStatus,
                    VersioningUtil.RavenDocumentRevision,
                    VersioningUtil.RavenDocumentParentRevision,
                    Constants.RavenCreateVersion
                };
            }
        }
    }
}
