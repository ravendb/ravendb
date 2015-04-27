//-----------------------------------------------------------------------
// <copyright file="smugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Smuggler;
using Raven.Smuggler.Client;

namespace Raven.Smuggler
{
	public class SmugglerDatabaseApi : SmugglerDatabaseApiBase
	{
		public SmugglerDatabaseApi(SmugglerDatabaseOptions options = null)
			: base(options ?? new SmugglerDatabaseOptions())
		{
			Operations = new SmugglerRemoteDatabaseOperations(() => store, () => operation, () => SupportedFeatures.IsDocsStreamingSupported, () => SupportedFeatures.IsTransformersSupported, () => SupportedFeatures.IsIdentitiesSmugglingSupported);
		}

		private BulkInsertOperation operation;

		private DocumentStore store;

        public override async Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions)
		{
			SetDatabaseNameIfEmpty(betweenOptions.From);
			SetDatabaseNameIfEmpty(betweenOptions.To);

	        using (var exportStore = CreateStore(betweenOptions.From))
	        using (var exportBulkOperation = CreateBulkInsertOperation(exportStore))
	        {
		        var exportStoreFeatures = new Reference<ServerSupportedFeatures>();
		        var exportOperations = new SmugglerRemoteDatabaseOperations(() => exportStore, () => exportBulkOperation, () => exportStoreFeatures.Value.IsDocsStreamingSupported, () => exportStoreFeatures.Value.IsTransformersSupported, () => exportStoreFeatures.Value.IsIdentitiesSmugglingSupported);

		        exportStoreFeatures.Value = await DetectServerSupportedFeatures(exportOperations, betweenOptions.From);

		        using (var importStore = CreateStore(betweenOptions.To))
				using (var importBulkOperation = CreateBulkInsertOperation(importStore))
		        {
					var importStoreFeatures = new Reference<ServerSupportedFeatures>();
					var importOperations = new SmugglerRemoteDatabaseOperations(() => importStore, () => importBulkOperation, () => importStoreFeatures.Value.IsDocsStreamingSupported, () => importStoreFeatures.Value.IsTransformersSupported, () => importStoreFeatures.Value.IsIdentitiesSmugglingSupported);

			        importStoreFeatures.Value = await DetectServerSupportedFeatures(importOperations, betweenOptions.To);

					await new SmugglerDatabaseBetweenOperation
					{
						OnShowProgress = betweenOptions.ReportProgress
					}
					.Between(new SmugglerBetweenOperations
					{
						From = exportOperations,
						To = importOperations,
						IncrementalKey = betweenOptions.IncrementalKey
					}, Options);
		        }
	        }
		}

        [Obsolete("Use RavenFS instead.")]
		protected override Task<Etag> ExportAttachments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
			if (maxEtag != null)
				throw new ArgumentException("We don't support maxEtag in SmugglerDatabaseApi", maxEtag);

			return base.ExportAttachments(src, jsonWriter, lastEtag, null);
		}

		public override Task ExportDeletions(JsonTextWriter jsonWriter, OperationState result, LastEtagsInfo maxEtagsToFetch)
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
						await operation.DisposeAsync();
					operation = CreateBulkInsertOperation(store);
					await base.ImportData(importOptions, stream);
				}
				finally
				{
					if (operation != null) 
						disposeTask = operation.DisposeAsync();
				}

				if (disposeTask != null)
				{
					await disposeTask;
				}
			}
		}

		private BulkInsertOperation CreateBulkInsertOperation(DocumentStore documentStore)
		{
			var result = new ChunkedBulkInsertOperation(documentStore.DefaultDatabase, documentStore, documentStore.Listeners, new BulkInsertOptions
			{
				BatchSize = Options.BatchSize,
				OverwriteExisting = true,
				Compression = Options.DisableCompressionOnImport ? BulkInsertCompression.None : BulkInsertCompression.GZip

			}, documentStore.Changes(), Options.ChunkSize, Options.TotalDocumentSizeInChunkLimitInBytes);

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
				Credentials = credentials ?? CredentialCache.DefaultNetworkCredentials
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
                return await base.ExportData(exportOptions);
            }
		}
	}
}