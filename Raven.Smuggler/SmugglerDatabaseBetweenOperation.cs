using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Database.Smuggler;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    internal static class SmugglerDatabaseBetweenOperation
	{
		const int RetriesCount = 5;

        public static async Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions, SmugglerDatabaseOptions databaseOptions)
		{
			SetDatabaseNameIfEmpty(betweenOptions.From);
			SetDatabaseNameIfEmpty(betweenOptions.To);

			using (var exportStore = CreateStore(betweenOptions.From))
			using (var importStore = CreateStore(betweenOptions.To))
			{
				SmugglerDatabaseApi.ValidateThatServerIsUpAndDatabaseExists(betweenOptions.From, exportStore);
				SmugglerDatabaseApi.ValidateThatServerIsUpAndDatabaseExists(betweenOptions.To, importStore);

				var exportBatchSize = GetBatchSize(exportStore, databaseOptions);
				var importBatchSize = GetBatchSize(importStore, databaseOptions);

				var exportStoreSupportedFeatures = await DetectServerSupportedFeatures(exportStore);
				var importStoreSupportedFeatures = await DetectServerSupportedFeatures(importStore);

				if (string.IsNullOrEmpty(betweenOptions.IncrementalKey))
				{
					betweenOptions.IncrementalKey = ((AsyncServerClient)exportStore.AsyncDatabaseCommands).Url;
				}

				var incremental = new ExportIncremental();
				if (databaseOptions.Incremental)
				{
					var jsonDocument = await importStore.AsyncDatabaseCommands.GetAsync(SmugglerExportIncremental.RavenDocumentKey);
					if (jsonDocument != null)
					{
						var smugglerExportIncremental = jsonDocument.DataAsJson.JsonDeserialization<SmugglerExportIncremental>();
						ExportIncremental value;
						if (smugglerExportIncremental.ExportIncremental.TryGetValue(betweenOptions.IncrementalKey, out value))
						{
							incremental = value;
						}

						databaseOptions.StartDocsEtag = incremental.LastDocsEtag ?? Etag.Empty;
						databaseOptions.StartAttachmentsEtag = incremental.LastAttachmentsEtag ?? Etag.Empty;
					}
				}

				if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Indexes))
				{
					await ExportIndexes(exportStore, importStore, exportBatchSize);
				}
				if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Transformers) && exportStoreSupportedFeatures.IsTransformersSupported && importStoreSupportedFeatures.IsTransformersSupported)
				{
					await ExportTransformers(exportStore, importStore, exportBatchSize);
				}
				if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Documents))
				{
					incremental.LastDocsEtag = await ExportDocuments(exportStore, importStore, databaseOptions, exportStoreSupportedFeatures, exportBatchSize, importBatchSize);
				}
				if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Attachments))
				{
					incremental.LastAttachmentsEtag = await ExportAttachments(exportStore, importStore, databaseOptions, exportBatchSize);
				}
				if (exportStoreSupportedFeatures.IsIdentitiesSmugglingSupported && importStoreSupportedFeatures.IsIdentitiesSmugglingSupported)
				{
                    await ExportIdentities(exportStore, importStore, databaseOptions.OperateOnTypes);
				}

				if (databaseOptions.Incremental)
				{
					var smugglerExportIncremental = new SmugglerExportIncremental();
					var jsonDocument = await importStore.AsyncDatabaseCommands.GetAsync(SmugglerExportIncremental.RavenDocumentKey);
					if (jsonDocument != null)
					{
						smugglerExportIncremental = jsonDocument.DataAsJson.JsonDeserialization<SmugglerExportIncremental>();
					}
					smugglerExportIncremental.ExportIncremental[betweenOptions.IncrementalKey] = incremental;
					await importStore.AsyncDatabaseCommands.PutAsync(SmugglerExportIncremental.RavenDocumentKey, null, RavenJObject.FromObject(smugglerExportIncremental), new RavenJObject());
				}
			}
		}

		private static async Task ExportIdentities(DocumentStore exportStore, DocumentStore importStore, ItemType operateOnTypes)
		{
			int start = 0;
			const int pageSize = 1024;
			long totalIdentitiesCount;
			var identities = new List<KeyValuePair<string, long>>();

			ShowProgress("Exporting Identities");

			do
			{
				var url = exportStore.Url.ForDatabase(exportStore.DefaultDatabase) + "/debug/identities?start=" + start + "&pageSize=" + pageSize;
				using (var request = exportStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, "GET", exportStore.DatabaseCommands.PrimaryCredentials, exportStore.Conventions)))
				{
					var identitiesInfo = (RavenJObject)await request.ReadResponseJsonAsync();
					totalIdentitiesCount = identitiesInfo.Value<long>("TotalCount");

					foreach (var identity in identitiesInfo.Value<RavenJArray>("Identities"))
					{
						identities.Add(new KeyValuePair<string, long>(identity.Value<string>("Key"), identity.Value<long>("Value")));
					}

					start += pageSize;
				}
			} while (identities.Count < totalIdentitiesCount);

			ShowProgress("Exported {0} following identities: {1}", identities.Count, string.Join(", ", identities.Select(x => x.Key)));

			var filteredIdentities = identities.Where(x =>
			{
				if ("Raven/Etag".Equals(x.Key, StringComparison.InvariantCultureIgnoreCase))
					return false;

				if ("IndexId".Equals(x.Key, StringComparison.InvariantCultureIgnoreCase) && operateOnTypes.HasFlag(ItemType.Indexes))
					return false;

				if (operateOnTypes.HasFlag(ItemType.Documents))
					return true;

				return false;
			}).ToList();

			ShowProgress("After filtering {0} identities need to be imported: {1}", filteredIdentities.Count, string.Join(", ", filteredIdentities.Select(x => x.Key)));

			foreach (var identityInfo in filteredIdentities)
			{
				importStore.DatabaseCommands.SeedIdentityFor(identityInfo.Key, identityInfo.Value);

				ShowProgress("Identity '{0}' imported with value {1}", identityInfo.Key, identityInfo.Value);
			}

			ShowProgress("Done with importing indentities");
		}

		private static int GetBatchSize(DocumentStore store, SmugglerDatabaseOptions databaseOptions)
		{
			if (store.HasJsonRequestFactory == false)
				return databaseOptions.BatchSize;

			var url = store.Url.ForDatabase(store.DefaultDatabase) + "/debug/config";
			try
			{
				using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, "GET", store.DatabaseCommands.PrimaryCredentials, store.Conventions)))
				{
					var configuration = (RavenJObject)request.ReadResponseJson();

					var maxNumberOfItemsToProcessInSingleBatch = configuration.Value<int>("MaxNumberOfItemsToProcessInSingleBatch");
					if (maxNumberOfItemsToProcessInSingleBatch <= 0) 
						return databaseOptions.BatchSize;

					return Math.Min(databaseOptions.BatchSize, maxNumberOfItemsToProcessInSingleBatch);
				}
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode == HttpStatusCode.Forbidden) // let it continue with the user defined batch size
					return databaseOptions.BatchSize;

				throw;
			}
		}

		private static void SetDatabaseNameIfEmpty(RavenConnectionStringOptions connection)
		{
			if (string.IsNullOrWhiteSpace(connection.DefaultDatabase) == false)
				return;
            
			var index = connection.Url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
			if (index != -1)
			{
				connection.DefaultDatabase = connection.Url.Substring(index + "/databases/".Length).Trim(new[] {'/'});
			}
		}

		private static async Task ExportIndexes(DocumentStore exportStore, DocumentStore importStore, int exportBatchSize)
		{
			var totalCount = 0;
			while (true)
			{
				var indexes = await exportStore.AsyncDatabaseCommands.GetIndexesAsync(totalCount, exportBatchSize);
				if (indexes.Length == 0)
				{
					ShowProgress("Done with reading indexes, total: {0}", totalCount);
					break;
				}
				totalCount += indexes.Length;
				ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
				foreach (var index in indexes)
				{
					var indexName = await importStore.AsyncDatabaseCommands.PutIndexAsync(index.Name, index, true);
					ShowProgress("Successfully PUT index '{0}'", indexName);
				}
			}
		}

		private static async Task<Etag> ExportDocuments(DocumentStore exportStore, DocumentStore importStore, SmugglerDatabaseOptions databaseOptions, ServerSupportedFeatures exportStoreSupportedFeatures, int exportBatchSize, int importBatchSize)
		{
			var now = SystemTime.UtcNow;

			string lastEtag = databaseOptions.StartDocsEtag;
			var totalCount = 0;
			var lastReport = SystemTime.UtcNow;
			var reportInterval = TimeSpan.FromSeconds(2);
			ShowProgress("Exporting Documents");

			var bulkInsertOperation = importStore.BulkInsert(null, new BulkInsertOptions
			                                                       {
																	   BatchSize = importBatchSize,
				                                                       OverwriteExisting = true,
			                                                       });
			bulkInsertOperation.Report += text => ShowProgress(text);
			var jintHelper = new SmugglerJintHelper();
			jintHelper.Initialize(databaseOptions);
			try
			{
				while (true)
				{
					if (exportStoreSupportedFeatures.IsDocsStreamingSupported)
					{
						ShowProgress("Streaming documents from " + lastEtag);
						using (var documentsEnumerator = await exportStore.AsyncDatabaseCommands.StreamDocsAsync(lastEtag))
						{
							while (await documentsEnumerator.MoveNextAsync())
							{
								var document = documentsEnumerator.Current;
								var metadata = document.Value<RavenJObject>("@metadata");
								var id = metadata.Value<string>("@id");
								var etag = Etag.Parse(metadata.Value<string>("@etag"));

								lastEtag = etag;

								if (!databaseOptions.MatchFilters(document))
									continue;
								if (databaseOptions.ShouldExcludeExpired && databaseOptions.ExcludeExpired(document, now))
									continue;

								if (databaseOptions.StripReplicationInformation) 
									document["@metadata"] = StripReplicationInformationFromMetadata(document["@metadata"] as RavenJObject);

								if(databaseOptions.ShouldDisableVersioningBundle)
									document["@metadata"] = DisableVersioning(document["@metadata"] as RavenJObject);

								if (!string.IsNullOrEmpty(databaseOptions.TransformScript))
								{
									document = jintHelper.Transform(databaseOptions.TransformScript, document);
									if(document == null)
										continue;
									metadata = document.Value<RavenJObject>("@metadata");
								}

								document.Remove("@metadata");
								bulkInsertOperation.Store(document, metadata, id);
								totalCount++;

								if (totalCount%1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
								{
									ShowProgress("Exported {0} documents", totalCount);
									lastReport = SystemTime.UtcNow;
								}
							}
						}
					}
					else
					{
						int retries = RetriesCount;
						var originalRequestTimeout = exportStore.JsonRequestFactory.RequestTimeout;
						var timeout = databaseOptions.Timeout.Seconds;
						if (timeout < 30)
							timeout = 30;
						try
						{
							var operationMetadata = new OperationMetadata(exportStore.Url, exportStore.Credentials, exportStore.ApiKey);

							while (true)
							{
								try
								{
									ShowProgress("Get documents from " + lastEtag);
									var documents = await ((AsyncServerClient)exportStore.AsyncDatabaseCommands).GetDocumentsInternalAsync(null, lastEtag, exportBatchSize, operationMetadata);
									foreach (RavenJObject document in documents)
									{
										var metadata = document.Value<RavenJObject>("@metadata");
										var id = metadata.Value<string>("@id");
										var etag = Etag.Parse(metadata.Value<string>("@etag"));
										lastEtag = etag;

										if (!databaseOptions.MatchFilters(document))
											continue;
										if (databaseOptions.ShouldExcludeExpired && databaseOptions.ExcludeExpired(document, now))
											continue;

										if (databaseOptions.StripReplicationInformation)
											document["@metadata"] = StripReplicationInformationFromMetadata(document["@metadata"] as RavenJObject);

										if (databaseOptions.ShouldDisableVersioningBundle)
											document["@metadata"] = DisableVersioning(document["@metadata"] as RavenJObject);

										document.Remove("@metadata");
										metadata.Remove("@id");
										metadata.Remove("@etag");

										bulkInsertOperation.Store(document, metadata, id);
										totalCount++;

										if (totalCount%1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
										{
											ShowProgress("Exported {0} documents", totalCount);
											lastReport = SystemTime.UtcNow;
										}
									}
									break;
								}
								catch (Exception e)
								{
									if (retries-- == 0)
										throw;
									exportStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(timeout *= 2);
									importStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(timeout *= 2);
									ShowProgress("Error reading from database, remaining attempts {0}, will retry. Error: {1}", retries, e);
								}
							}
						}
						finally
						{
							exportStore.JsonRequestFactory.RequestTimeout = originalRequestTimeout;
						}
					}

					// In a case that we filter all the results, the formEtag hasn't updaed to the latest, 
					// but we still need to continue until we finish all the docs.
					var databaseStatistics = await exportStore.AsyncDatabaseCommands.GetStatisticsAsync();
					var lastEtagComparable = new ComparableByteArray(lastEtag);
					if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
					{
						lastEtag = EtagUtil.Increment(lastEtag, exportBatchSize);
						ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
						continue;
					}

					ShowProgress("Done with reading documents, total: {0}", totalCount);
					return lastEtag;
				}
			}
			finally
			{
				bulkInsertOperation.Dispose();
			}
		}

        [Obsolete("Use RavenFS instead.")]
		private async static Task<Etag> ExportAttachments(DocumentStore exportStore, DocumentStore importStore, SmugglerDatabaseOptions databaseOptions, int exportBatchSize)
		{
			Etag lastEtag = databaseOptions.StartAttachmentsEtag;
			int totalCount = 0;
			while (true)
			{
				var attachments = await exportStore.AsyncDatabaseCommands.GetAttachmentsAsync(0, lastEtag, exportBatchSize);
				if (attachments.Length == 0)
				{
					var databaseStatistics = await exportStore.AsyncDatabaseCommands.GetStatisticsAsync();
					var lastEtagComparable = new ComparableByteArray(lastEtag);
					if (lastEtagComparable.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
					{
						lastEtag = EtagUtil.Increment(lastEtag, exportBatchSize);
						ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}", lastEtag);
						continue;
					}
					ShowProgress("Done with reading attachments, total: {0}", totalCount);
					return lastEtag;
				}

				totalCount += attachments.Length;
				ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachments.Length, totalCount);
				foreach (var attachmentInformation in attachments)
				{
					if (databaseOptions.StripReplicationInformation)
						attachmentInformation.Metadata = StripReplicationInformationFromMetadata(attachmentInformation.Metadata);

					ShowProgress("Downloading attachment: {0}", attachmentInformation.Key);

					var attachment = await exportStore.AsyncDatabaseCommands.GetAttachmentAsync(attachmentInformation.Key);
					await importStore.AsyncDatabaseCommands.PutAttachmentAsync(attachment.Key, null, attachment.Data(), attachment.Metadata);
				}

				lastEtag = Etag.Parse(attachments.Last().Etag);
			}
		}

		private static async Task ExportTransformers(DocumentStore exportStore, DocumentStore importStore, int exportBatchSize)
		{
			var totalCount = 0;
			while (true)
			{
				var transformers = await exportStore.AsyncDatabaseCommands.GetTransformersAsync(totalCount, exportBatchSize);
				if (transformers.Length == 0)
				{
					ShowProgress("Done with reading transformers, total: {0}", totalCount);
					break;
				}
				totalCount += transformers.Length;
				ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Length, totalCount);
				foreach (var transformer in transformers)
				{
					var transformerName = await importStore.AsyncDatabaseCommands.PutTransformerAsync(transformer.Name, transformer);
					ShowProgress("Successfully PUT transformer '{0}'", transformerName);
				}
			}
		}

		private static DocumentStore CreateStore(RavenConnectionStringOptions connection)
		{
			var store = new DocumentStore
			            {
				            Url = connection.Url,
				            ApiKey = connection.ApiKey,
				            Credentials = connection.Credentials,
				            DefaultDatabase = connection.DefaultDatabase,
				            Conventions =
				            {
					            FailoverBehavior = FailoverBehavior.FailImmediately,
					            ShouldCacheRequest = s => false,
					            ShouldAggressiveCacheTrackChanges = false,
					            ShouldSaveChangesForceAggressiveCacheCheck = false,
				            }
			            };
			store.Initialize(ensureDatabaseExists: false);
			store.JsonRequestFactory.DisableAllCaching();
			return store;
		}

		private static async Task<ServerSupportedFeatures> DetectServerSupportedFeatures(DocumentStore store)
		{
			var buildNumber = await store.AsyncDatabaseCommands.GlobalAdmin.GetBuildNumberAsync();
			if (buildNumber == null || string.IsNullOrEmpty(buildNumber.ProductVersion))
			{
				ShowProgress("Server version is not available. Running in legacy mode which does not support transformers, documents streaming and identities smuggling.");
				return new ServerSupportedFeatures
				       {
					       IsTransformersSupported = false,
					       IsDocsStreamingSupported = false,
						   IsIdentitiesSmugglingSupported = false,
				       };
			}	

			var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<SmugglerDatabaseApiBase>()).ProductVersion;
			var subSmugglerVersion = smugglerVersion.Substring(0, 3);

			var subServerVersion = buildNumber.ProductVersion.Substring(0, 3);
			var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));

			if (intServerVersion < 25)
			{
				ShowProgress("Running in legacy mode, importing/exporting transformers and identities is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
				return new ServerSupportedFeatures
				       {
					       IsTransformersSupported = false,
					       IsDocsStreamingSupported = false,
						   IsIdentitiesSmugglingSupported = false,
				       };
			}

			if (intServerVersion == 25)
			{
				ShowProgress("Running in legacy mode, importing/exporting identities is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
				return new ServerSupportedFeatures
				{
					IsTransformersSupported = true,
					IsDocsStreamingSupported = true,
					IsIdentitiesSmugglingSupported = false,
				};
			}

			return new ServerSupportedFeatures
			       {
				       IsTransformersSupported = true,
				       IsDocsStreamingSupported = true,
					   IsIdentitiesSmugglingSupported = true,
			       };
		}

		// [StringFormatMethod("format")]
		private static void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public static RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata)
		{
			if (metadata != null)
			{
				metadata.Remove(Constants.RavenReplicationHistory);
				metadata.Remove(Constants.RavenReplicationSource);
				metadata.Remove(Constants.RavenReplicationVersion);
			}

			return metadata;
		}

		public static RavenJToken DisableVersioning(RavenJObject metadata)
		{
			metadata.Add(Constants.RavenIgnoreVersioning, true);

			return metadata;
		}
	}
}