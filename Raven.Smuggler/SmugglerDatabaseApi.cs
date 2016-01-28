//-----------------------------------------------------------------------
// <copyright file="smugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;

using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Raven.Smuggler
{
    public class SmugglerDatabaseApi : SmugglerDatabaseApiBase
    {
        public SmugglerDatabaseApi(SmugglerDatabaseOptions options = null)
            : base(options ?? new SmugglerDatabaseOptions())
        {
            Operations = new SmugglerRemoteDatabaseOperations(() => store, () => operation, () => IsDocsStreamingSupported, () => IsTransformersSupported, () => IsIdentitiesSmugglingSupported);			
        }

        protected BulkInsertOperation operation;

        protected DocumentStore store;

        public override Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions)
        {
            return SmugglerDatabaseBetweenOperation.Between(betweenOptions, Options);
        }

        [Obsolete("Use RavenFS instead.")]
        protected override Task<ExportOperationStatus> ExportAttachments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag, int maxNumberOfAttachmentsToExport)
        {
            if (maxEtag != null)
                throw new ArgumentException("We don't support maxEtag in SmugglerDatabaseApi", maxEtag);

            return base.ExportAttachments(src, jsonWriter, lastEtag, null, maxNumberOfAttachmentsToExport);
        }

        public override Task ExportDeletions(JsonTextWriter jsonWriter, OperationState result, LastEtagsInfo maxEtagsToFetch)
        {
            throw new NotSupportedException("Exporting deletions is not supported for Command Line Smuggler");
        }

        public override async Task ImportData(SmugglerImportOptions<RavenConnectionStringOptions> importOptions, Stream stream)
        {
            using (store = CreateStore(importOptions.To))
            {
                Task disposeTask;

                try
                {
                    await CreateBulkInsertOperation();

                    await base.ImportData(importOptions, stream);
                }
                finally
                {
                    disposeTask = operation.DisposeAsync();
                }

                if (disposeTask != null)
                {
                    await disposeTask;
                }
            }
        }

        public override async Task<OperationState> ExportData(SmugglerExportOptions<RavenConnectionStringOptions> exportOptions)
        {
            using (store = CreateStore(exportOptions.From))
            {
                return await base.ExportData(exportOptions);
            }
        }

        private async Task CreateBulkInsertOperation()
        {
            if (operation != null)
                await operation.DisposeAsync();

            operation = new BulkInsertOperation(store.DefaultDatabase, store, store.Listeners, new BulkInsertOptions
            {
                BatchSize = Options.BatchSize,
                OverwriteExisting = true,
                ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
                {
                    MaxDocumentsPerChunk = Options.ChunkSize,
                    MaxChunkVolumeInBytes = Options.TotalDocumentSizeInChunkLimitInBytes.HasValue?Options.TotalDocumentSizeInChunkLimitInBytes.Value:0
                }
            }, store.Changes());

            operation.Report += text => Operations.ShowProgress(text);
        }

        private static DocumentStore CreateStore(RavenConnectionStringOptions connectionStringOptions)
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
                Credentials = credentials ?? CredentialCache.DefaultNetworkCredentials
            };

            s.Initialize();

            ValidateThatServerIsUpAndDatabaseExists(connectionStringOptions, s);

            s.DefaultDatabase = connectionStringOptions.DefaultDatabase;

            return s;
        }

        internal static void ValidateThatServerIsUpAndDatabaseExists(RavenConnectionStringOptions server, DocumentStore s)
        {
            var shouldDispose = false;

            try
            {
                var commands = !string.IsNullOrEmpty(server.DefaultDatabase)
                                   ? s.DatabaseCommands.ForDatabase(server.DefaultDatabase)
                                   : s.DatabaseCommands;

                commands.GetStatistics(); // check if database exist
            }
            catch (Exception e)
            {
                shouldDispose = true;

                var responseException = e as ErrorResponseException;
                if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && responseException.Message.StartsWith("Could not find a database named"))
                    throw new SmugglerException(
                        string.Format(
                            "Smuggler does not support database creation (database '{0}' on server '{1}' must exist before running Smuggler).",
                            server.DefaultDatabase,
                            s.Url), e);


                if (e.InnerException != null)
                {
                    var webException = e.InnerException as WebException;
                    if (webException != null)
                    {
                        throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", webException.Message), webException);
                    }
                } throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", e.Message), e);
            }
            finally
            {
                if (shouldDispose)
                    s.Dispose();
            }
        }
    }
}
