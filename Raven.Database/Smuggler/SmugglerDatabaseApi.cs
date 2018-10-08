//-----------------------------------------------------------------------
// <copyright file="smugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Document;
using Raven.Database.Smuggler;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler
{
    public class SmugglerDatabaseApi : SmugglerDatabaseApiBase
    {
        public SmugglerDatabaseApi(SmugglerDatabaseOptions options = null)
            : base(options ?? new SmugglerDatabaseOptions())
        {
            Operations = new SmugglerRemoteDatabaseOperations(() => store, () => operation, () => SupportedFeatures.IsDocsStreamingSupported, () => SupportedFeatures.IsTransformersSupported, () => SupportedFeatures.IsIdentitiesSmugglingSupported);
        }

        protected BulkInsertOperation operation;

        protected DocumentStore store;

        public override async Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions)
        {
            SetDatabaseNameIfEmpty(betweenOptions.From);
            SetDatabaseNameIfEmpty(betweenOptions.To);

            using (var exportStore = CreateStore(betweenOptions.From))
            {
                var exportStoreFeatures = new Reference<ServerSupportedFeatures>();
                var exportOperations = new SmugglerRemoteDatabaseOperations(() => exportStore,
                    () =>
                    {
                        throw new NotSupportedException("Could not and should not open bulk insert to origin of the smuggling operation");
                    }, 
                    () => exportStoreFeatures.Value.IsDocsStreamingSupported, 
                    () => exportStoreFeatures.Value.IsTransformersSupported, 
                    () => exportStoreFeatures.Value.IsIdentitiesSmugglingSupported);

                exportStoreFeatures.Value = await DetectServerSupportedFeatures(exportOperations, betweenOptions.From).ConfigureAwait(false);

                using (var importStore = CreateStore(betweenOptions.To))
                using (var importBulkOperation = CreateBulkInsertOperation(importStore))
                {
                    var importStoreFeatures = new Reference<ServerSupportedFeatures>();
                    var importOperations = new SmugglerRemoteDatabaseOperations(() => importStore, () => importBulkOperation, () => importStoreFeatures.Value.IsDocsStreamingSupported, () => importStoreFeatures.Value.IsTransformersSupported, () => importStoreFeatures.Value.IsIdentitiesSmugglingSupported);

                    importStoreFeatures.Value = await DetectServerSupportedFeatures(importOperations, betweenOptions.To).ConfigureAwait(false);

                    await new SmugglerDatabaseBetweenOperation
                    {
                        OnShowProgress = betweenOptions.ReportProgress
                    }
                    .Between(new SmugglerBetweenOperations
                    {
                        From = exportOperations,
                        To = importOperations,
                        IncrementalKey = betweenOptions.IncrementalKey
                    }, Options, null)
                    .ConfigureAwait(false);
                }
            }
        }

        [Obsolete("Use RavenFS instead.")]
        protected override Task<ExportOperationStatus> ExportAttachments(RavenConnectionStringOptions src, SmugglerJsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag, int maxNumberOfAttachmentsToExport)
        {
            if (maxEtag != null)
                throw new ArgumentException("We don't support maxEtag in SmugglerDatabaseApi", maxEtag);

            return base.ExportAttachments(src, jsonWriter, lastEtag, null, maxNumberOfAttachmentsToExport);
        }

        public override Task ExportDeletions(SmugglerJsonTextWriter jsonWriter, OperationState result, LastEtagsInfo maxEtagsToFetch)
        {
            throw new NotSupportedException("Exporting deletions is not supported for Command Line Smuggler");
        }

        public override async Task ImportData(SmugglerImportOptions<RavenConnectionStringOptions> importOptions, Stream stream)
        {
            using (store = CreateStore(importOptions.To))
            {
                Task disposeTask = null;

                try
                {
                    if (operation != null)
                        await operation.DisposeAsync().ConfigureAwait(false);
                    operation = CreateBulkInsertOperation(store);
                    await base.ImportData(importOptions, stream).ConfigureAwait(false);
                }
                finally
                {
                    if (operation != null) 
                    disposeTask = operation.DisposeAsync();
                }

                if (disposeTask != null)
                {
                    await disposeTask.ConfigureAwait(false);
                }
            }
        }

        private BulkInsertOperation CreateBulkInsertOperation(DocumentStore documentStore)
        {
            var result = documentStore.BulkInsert(documentStore.DefaultDatabase, new BulkInsertOptions
            {
                BatchSize = Options.BatchSize,
                OverwriteExisting = true,
                Compression = Options.DisableCompressionOnImport ? BulkInsertCompression.None : BulkInsertCompression.GZip,
                ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                {
                    MaxChunkVolumeInBytes = Options.TotalDocumentSizeInChunkLimitInBytes,
                    MaxDocumentsPerChunk = Options.ChunkSize
                },
                WriteTimeoutMilliseconds =  (int)Options.Timeout.TotalMilliseconds
            });

            result.Report += text => Operations.ShowProgress(text);

            return result;
        }

        protected static DocumentStore CreateStore(RavenConnectionStringOptions connectionStringOptions)
        {
            var credentials = connectionStringOptions.Credentials as NetworkCredential;
            if (credentials != null && //precaution
                (String.IsNullOrWhiteSpace(credentials.UserName) ||
                 String.IsNullOrWhiteSpace(credentials.Password)))
            {
                credentials = CredentialCache.DefaultNetworkCredentials;
            }

            var s = new DocumentStore
            {
                Url = connectionStringOptions.Url,
                ApiKey = connectionStringOptions.ApiKey,
                Credentials = credentials ?? CredentialCache.DefaultNetworkCredentials,
                AvoidCluster = true
            };

            s.Initialize();

            ServerValidation.ValidateThatServerIsUpAndDatabaseExists(connectionStringOptions, s);

            s.DefaultDatabase = connectionStringOptions.DefaultDatabase;

            return s;
        }

        public override async Task<OperationState> ExportData(SmugglerExportOptions<RavenConnectionStringOptions> exportOptions)
        {
            using (store = CreateStore(exportOptions.From))
            {
                return await base.ExportData(exportOptions).ConfigureAwait(false);
            }
        }
    }
}
