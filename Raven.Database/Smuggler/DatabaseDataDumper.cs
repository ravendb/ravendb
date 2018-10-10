// -----------------------------------------------------------------------
//  <copyright file="DatabaseDataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Document;
using Raven.Smuggler;

namespace Raven.Database.Smuggler
{
    public class DatabaseDataDumper : SmugglerDatabaseApiBase
    {
        public DatabaseDataDumper(DocumentDatabase database, SmugglerDatabaseOptions options = null)
            : base(options ?? new SmugglerDatabaseOptions())
        {
            Operations = new SmugglerEmbeddedDatabaseOperations(database);
        }

        public override async Task ExportDeletions(SmugglerJsonTextWriter jsonWriter, OperationState result, LastEtagsInfo maxEtagsToFetch)
        {
            jsonWriter.WritePropertyName("DocsDeletions");
            jsonWriter.WriteStartArray();
            result.LastDocDeleteEtag = await Operations.ExportDocumentsDeletion(jsonWriter, result.LastDocDeleteEtag, maxEtagsToFetch.LastDocDeleteEtag.IncrementBy(1)).ConfigureAwait(false);
            jsonWriter.WriteEndArray();

            jsonWriter.WritePropertyName("AttachmentsDeletions");
            jsonWriter.WriteStartArray();
            result.LastAttachmentsDeleteEtag = await Operations.ExportAttachmentsDeletion(jsonWriter, result.LastAttachmentsDeleteEtag, maxEtagsToFetch.LastAttachmentsDeleteEtag.IncrementBy(1)).ConfigureAwait(false);
            jsonWriter.WriteEndArray();
        }

        public override async Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions)
        {
            if (betweenOptions.From != null)
            {
                throw new ArgumentException("Data dumper supports to smuggle data just from the related database. 'From' parameter has be 'null' because it's automatically set to the specified database.", "betweenOptions.From");
            }

            SetDatabaseNameIfEmpty(betweenOptions.To);

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
                    From = Operations,
                    To = importOperations,
                    IncrementalKey = betweenOptions.IncrementalKey
                }, Options)
                .ConfigureAwait(false);
            }
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
                }
            });

            result.Report += text => Operations.ShowProgress(text);

            return result;
        }

        public Action<string> Progress
        {
            get
            {
                return ((SmugglerEmbeddedDatabaseOperations)Operations).Progress;
            }

            set
            {
                ((SmugglerEmbeddedDatabaseOperations)Operations).Progress = value;
            }
        }
    }
}
