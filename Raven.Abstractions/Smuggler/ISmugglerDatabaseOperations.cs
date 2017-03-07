// -----------------------------------------------------------------------
//  <copyright file="ISmugglerDatabaseOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Database.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public interface ISmugglerDatabaseOperations
    {
        SmugglerDatabaseOptions Options { get; }

        [Obsolete("Use RavenFS instead.")]
        Task DeleteAttachment(string key);

        Task DeleteDocument(string key);

        [Obsolete("Use RavenFS instead.")]
        Task<Etag> ExportAttachmentsDeletion(SmugglerJsonTextWriter jsonWriter, Etag startAttachmentsDeletionEtag, Etag maxAttachmentEtag);

        Task<Etag> ExportDocumentsDeletion(SmugglerJsonTextWriter jsonWriter, Etag startDocsEtag, Etag maxEtag);

        LastEtagsInfo FetchCurrentMaxEtags();

        [Obsolete("Use RavenFS instead.")]
        Task<List<AttachmentInformation>> GetAttachments(int start, Etag etag, int maxRecords);

        [Obsolete("Use RavenFS instead.")]
        Task<byte[]> GetAttachmentData(AttachmentInformation attachmentInformation);

        JsonDocument GetDocument(string key);

        Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag, int take);

        Task<RavenJArray> GetIndexes(int totalCount);

        Task<DatabaseStatistics> GetStats();

        Task<RavenJArray> GetTransformers(int start);

        Task<BuildNumber> GetVersion(RavenConnectionStringOptions server);

        void PurgeTombstones(OperationState result);

        [Obsolete("Use RavenFS instead.")]
        Task PutAttachment(AttachmentExportInfo attachmentExportInfo);

        Task PutDocument(RavenJObject document, int size);

        Task PutIndex(string indexName, RavenJToken index);

        Task PutTransformer(string transformerName, RavenJToken transformer);

        void ShowProgress(string format, params object[] args);

        Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript);

        RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata);

        void Initialize(SmugglerDatabaseOptions options);

        void Configure(SmugglerDatabaseOptions options);

        Task SeedIdentityFor(string identityName, long identityValue);

        Task<IAsyncEnumerator<RavenJObject>> ExportItems(ItemType types, OperationState state);

        string GetIdentifier();

        Task<List<KeyValuePair<string, long>>> GetIdentities();

        Task SeedIdentities(List<KeyValuePair<string, long>> identities);

        Task WaitForLastBulkInsertTaskToFinish();
    }
}
