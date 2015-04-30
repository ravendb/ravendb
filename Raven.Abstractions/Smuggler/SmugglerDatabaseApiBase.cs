using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;

using System.IO.Compression;

using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Smuggler
{
    public abstract class SmugglerDatabaseApiBase : ISmugglerApi<RavenConnectionStringOptions, SmugglerDatabaseOptions, OperationState>
	{
		public ISmugglerDatabaseOperations Operations { get; protected set; }

        public SmugglerDatabaseOptions Options { get; private set; }

        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        protected SmugglerDatabaseApiBase(SmugglerDatabaseOptions options)
		{
			Options = options;
		}

        public virtual async Task<OperationState> ExportData(SmugglerExportOptions<RavenConnectionStringOptions> exportOptions)
		{
            Operations.Configure(Options);
            Operations.Initialize(Options);

            var result = new OperationState
            {
                FilePath = exportOptions.ToFile,
                LastAttachmentsEtag = Options.StartAttachmentsEtag,
                LastDocsEtag = Options.StartDocsEtag,
                LastDocDeleteEtag = Options.StartDocsDeletionEtag,
                LastAttachmentsDeleteEtag = Options.StartAttachmentsDeletionEtag
            };

            if (Options.Incremental)
			{
				if (Directory.Exists(result.FilePath) == false)
				{
					if (File.Exists(result.FilePath))
						result.FilePath = Path.GetDirectoryName(result.FilePath) ?? result.FilePath;
					else
						Directory.CreateDirectory(result.FilePath);
				}

                if (Options.StartDocsEtag == Etag.Empty && Options.StartAttachmentsEtag == Etag.Empty)
				{
					ReadLastEtagsFromFile(result);
				}

                result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(result.FilePath))
				{
					var counter = 1;
					while (true)
					{
                        result.FilePath = Path.Combine(Path.GetDirectoryName(result.FilePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".ravendb-incremental-dump");

						if (File.Exists(result.FilePath) == false)
							break;
						counter++;
					}
				}
			}

			SmugglerExportException lastException = null;

			bool ownedStream = exportOptions.ToStream == null;
			var stream = exportOptions.ToStream ?? File.Create(result.FilePath);

			try
			{
				SupportedFeatures = await DetectServerSupportedFeatures(Operations, exportOptions.From);
			}
			catch (WebException e)
			{
				Operations.ShowProgress("Failed to query server for supported features. Reason : " + e.Message);
				SupportedFeatures = GetLegacyModeFeatures(); //could not detect supported features, then run in legacy mode
				//				lastException = new SmugglerExportException
				//				{
				//					LastEtag = Etag.Empty,
				//					File = ownedStream ? result.FilePath : null
				//				};
			}

			try
			{
				using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
				using (var streamWriter = new StreamWriter(gZipStream))
				{
					var jsonWriter = new JsonTextWriter(streamWriter)
					{
						Formatting = Formatting.Indented
					};
					jsonWriter.WriteStartObject();
					jsonWriter.WritePropertyName("Indexes");
					jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Indexes))
					{
						await ExportIndexes(exportOptions.From, jsonWriter);
					}
					jsonWriter.WriteEndArray();

                    // used to synchronize max returned values for put/delete operations
                    var maxEtags = Operations.FetchCurrentMaxEtags();

					jsonWriter.WritePropertyName("Docs");
					jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Documents))
					{
						try
						{
							result.LastDocsEtag = await ExportDocuments(exportOptions.From, jsonWriter, result.LastDocsEtag, maxEtags.LastDocsEtag);
						}
                        catch (SmugglerExportException e)
                        {
                            result.LastDocsEtag = e.LastEtag;
                            e.File = ownedStream ? result.FilePath : null;
                            lastException = e;
                        }
					}
					jsonWriter.WriteEndArray();

					jsonWriter.WritePropertyName("Attachments");
					jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Attachments) && lastException == null)
					{
						try
						{
							result.LastAttachmentsEtag = await ExportAttachments(exportOptions.From, jsonWriter, result.LastAttachmentsEtag, maxEtags.LastAttachmentsEtag);
						}
						catch (SmugglerExportException e)
						{
							result.LastAttachmentsEtag = e.LastEtag;
							e.File = ownedStream ? result.FilePath : null;
							lastException = e;
						}
					}
					jsonWriter.WriteEndArray();

					jsonWriter.WritePropertyName("Transformers");
					jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Transformers) && lastException == null)
					{
						await ExportTransformers(exportOptions.From, jsonWriter);
					}
					jsonWriter.WriteEndArray();

                    if (Options.ExportDeletions)
					{
						await ExportDeletions(jsonWriter, result, maxEtags);
					}

					await ExportIdentities(jsonWriter, Options.OperateOnTypes);

					jsonWriter.WriteEndObject();
					streamWriter.Flush();
				}

                if (Options.Incremental)
                    WriteLastEtagsToFile(result, result.FilePath, IncrementalExportStateFile);

                if (Options.ExportDeletions)
				{
					Operations.PurgeTombstones(result);
				}

				if (lastException != null)
					throw lastException;

				return result;
			}
            finally
            {
                if (ownedStream && stream != null)
                    stream.Dispose();
            }
		}

		private async Task ExportIdentities(JsonTextWriter jsonWriter, ItemType operateOnTypes)
		{
			Operations.ShowProgress("Exporting Identities");

			var identities = await Operations.GetIdentities();

			Operations.ShowProgress("Exported {0} following identities: {1}", identities.Count, string.Join(", ", identities.Select(x => x.Key)));

			var filteredIdentities = identities.Where(x => FilterIdentity(x.Key, operateOnTypes)).ToList();

			Operations.ShowProgress("After filtering {0} identities need to be exported: {1}", filteredIdentities.Count, string.Join(", ", filteredIdentities.Select(x => x.Key)));

			jsonWriter.WritePropertyName("Identities");
			jsonWriter.WriteStartArray();

			foreach (var identityInfo in filteredIdentities)
			{
				new RavenJObject
						{
							{ "Key", identityInfo.Key }, 
							{ "Value", identityInfo.Value }
						}.WriteTo(jsonWriter);
		    }

			jsonWriter.WriteEndArray();

			Operations.ShowProgress("Done with exporting identities");
		}

		public bool FilterIdentity(string indentityName, ItemType operateOnTypes)
		{
			if ("Raven/Etag".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
				return false;

			if ("IndexId".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
				return false;

			if (operateOnTypes.HasFlag(ItemType.Documents))
				return true;

			return false;
		}

        public static void ReadLastEtagsFromFile(OperationState result, string etagFileLocation)
        {
            var log = LogManager.GetCurrentClassLogger();            

            if (!File.Exists(etagFileLocation))
                return;

            using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                RavenJObject ravenJObject;
                try
                {
                    ravenJObject = RavenJObject.Load(jsonReader);
                }
                catch (Exception e)
                {
                    log.WarnException("Could not parse etag document from file : " + etagFileLocation + ", ignoring, will start from scratch", e);
                    return;
                }
                result.LastDocsEtag = Etag.Parse(ravenJObject.Value<string>("LastDocEtag"));
                result.LastAttachmentsEtag = Etag.Parse(ravenJObject.Value<string>("LastAttachmentEtag"));
                result.LastDocDeleteEtag = Etag.Parse(ravenJObject.Value<string>("LastDocDeleteEtag") ?? Etag.Empty.ToString());
                result.LastAttachmentsDeleteEtag = Etag.Parse(ravenJObject.Value<string>("LastAttachmentsDeleteEtag") ?? Etag.Empty.ToString());
            }
        }

        public static void ReadLastEtagsFromFile(OperationState result)
		{            
            var etagFileLocation = Path.Combine(result.FilePath, IncrementalExportStateFile);
            ReadLastEtagsFromFile(result, etagFileLocation);
		}

        public static void WriteLastEtagsToFile(OperationState result, string etagFileLocation)
        {
            using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
            {
                new RavenJObject
					{
						{"LastDocEtag", result.LastDocsEtag.ToString()},
						{"LastAttachmentEtag", result.LastAttachmentsEtag.ToString()},
                        {"LastDocDeleteEtag", result.LastDocDeleteEtag.ToString()},
                        {"LastAttachmentsDeleteEtag", result.LastAttachmentsDeleteEtag.ToString()}
					}.WriteTo(new JsonTextWriter(streamWriter));
                streamWriter.Flush();
            }
        }
        public static void WriteLastEtagsToFile(OperationState result, string backupPath, string filename)
		{
			// ReSharper disable once AssignNullToNotNullAttribute
            var etagFileLocation = Path.Combine(Path.GetDirectoryName(backupPath), filename);
            WriteLastEtagsToFile(result, etagFileLocation);
		}

		private async Task ExportTransformers(RavenConnectionStringOptions src, JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				var transformers = await Operations.GetTransformers(totalCount);
				if (transformers.Length == 0)
				{
					Operations.ShowProgress("Done with reading transformers, total: {0}", totalCount);
					break;
				}

				totalCount += transformers.Length;
				Operations.ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Length, totalCount);

				foreach (var transformer in transformers)
				{
					transformer.WriteTo(jsonWriter);
				}
			}
		}

		public abstract Task ExportDeletions(JsonTextWriter jsonWriter, OperationState result, LastEtagsInfo maxEtagsToFetch);

        [Obsolete("Use RavenFS instead.")]
		protected virtual async Task<Etag> ExportAttachments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
			var totalCount = 0;
			var maxEtagReached = false;
			while (true)
			{
				try
				{
                    if (Options.Limit - totalCount <= 0 || maxEtagReached)
					{
						Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount);
						return lastEtag;
					}
                    var maxRecords = Math.Min(Options.Limit - totalCount, Options.BatchSize);
					var attachments = await Operations.GetAttachments(totalCount, lastEtag, maxRecords);
					if (attachments.Count == 0)
					{
						var databaseStatistics = await Operations.GetStats();
						if (lastEtag == null) lastEtag = Etag.Empty;
						if (lastEtag.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
						{
							lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
							Operations.ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}",
										 lastEtag);
							continue;
						}
						Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount);
						return lastEtag;
					}
					totalCount += attachments.Count;
					Operations.ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachments.Count, totalCount);
					foreach (var attachment in attachments)
					{
						if (maxEtag != null && attachment.Etag.CompareTo(maxEtag) > 0)
						{
							maxEtagReached = true;
							break;
						}

						var attachmentData = await Operations.GetAttachmentData(attachment);
						if (attachmentData == null)
							continue;

						new RavenJObject
						{
							{ "Data", attachmentData }, 
							{ "Metadata", attachment.Metadata }, 
							{ "Key", attachment.Key }, 
							{ "Etag", new RavenJValue(attachment.Etag.ToString()) }
						}.WriteTo(jsonWriter);

						lastEtag = attachment.Etag;
					}
				}
				catch (Exception e)
				{
					Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
					Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount, lastEtag);
					throw new SmugglerExportException(e.Message, e)
					{
						LastEtag = lastEtag,
					};
				}
			}
		}

        protected async Task<Etag> ExportDocuments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
			var now = SystemTime.UtcNow;
			var totalCount = 0;
			var lastReport = SystemTime.UtcNow;
			var reportInterval = TimeSpan.FromSeconds(2);
			var reachedMaxEtag = false;
			Operations.ShowProgress("Exporting Documents");
           
			while (true)
			{
				bool hasDocs = false;
				try
				{
                    var maxRecords = Options.Limit - totalCount;
					if (maxRecords > 0 && reachedMaxEtag == false)
					{
						var amountToFetchFromServer = Math.Min(Options.BatchSize, maxRecords);
						using (var documents = await Operations.GetDocuments(lastEtag, amountToFetchFromServer))
						{
							while (await documents.MoveNextAsync())
							{
								hasDocs = true;
								var document = documents.Current;

                                var tempLastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));

                                Debug.Assert(!String.IsNullOrWhiteSpace(document.Value<RavenJObject>("@metadata").Value<string>("@id")));

                                if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                                {
                                    reachedMaxEtag = true;
                                    break;
                                }
                                lastEtag = tempLastEtag;

                                if (!Options.MatchFilters(document))
									continue;

                                if (Options.ShouldExcludeExpired && Options.ExcludeExpired(document, now))
									continue;

								document.WriteTo(jsonWriter);
								totalCount++;

                                if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                {
                                    Operations.ShowProgress("Exported {0} documents", totalCount);
                                    lastReport = SystemTime.UtcNow;
                                }
							}
						}

						if (hasDocs)
							continue;

						// The server can filter all the results. In this case, we need to try to go over with the next batch.
						// Note that if the ETag' server restarts number is not the same, this won't guard against an infinite loop.
						// (This code provides support for legacy RavenDB version: 1.0)
						var databaseStatistics = await Operations.GetStats();
						var lastEtagComparable = new ComparableByteArray(lastEtag);
						if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
						{
							lastEtag = EtagUtil.Increment(lastEtag, amountToFetchFromServer);
							if (lastEtag.CompareTo(databaseStatistics.LastDocEtag) >= 0)
							{
								lastEtag = databaseStatistics.LastDocEtag;
							}
							Operations.ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
							continue;
						}
					}
				}
				catch (Exception e)
				{
					Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
					Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
					throw new SmugglerExportException(e.Message, e)
					{
						LastEtag = lastEtag,
					};
				}

				// Load HiLo documents for selected collections
                Options.Filters.ForEach(filter =>
				{
					if (filter.Path == "@metadata.Raven-Entity-Name")
					{
						filter.Values.ForEach(collectionName =>
						{
							JsonDocument doc = Operations.GetDocument("Raven/Hilo/" + collectionName);
							if (doc != null)
							{
								doc.Metadata["@id"] = doc.Key;
								doc.ToJson().WriteTo(jsonWriter);
								totalCount++;
							}
						});
					}
				});

                Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
				return lastEtag;
			}
		}

        public async Task WaitForIndexingAsOfLastWrite()
        {
            var stopwatch = Stopwatch.StartNew();
            var justIndexingWait = Stopwatch.StartNew();
                       
            var stats = await Operations.GetStats();
            
            int tries = 0;
            Etag cutOffEtag = stats.LastDocEtag;
            while (true)
            {
                if (stats.Indexes.All(x => x.LastIndexedEtag.CompareTo(cutOffEtag) >= 0))
                {
                    Operations.ShowProgress("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
                    break;
                }

                if (tries++ % 10 == 0)
                    Operations.ShowProgress("\rWaiting {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);

                Thread.Sleep(1000);
                stats = await Operations.GetStats();
            }

            stopwatch.Stop();
            justIndexingWait.Stop();
        }
        

		public async Task WaitForIndexing()
		{
            var stopwatch = Stopwatch.StartNew();
            var justIndexingWait = Stopwatch.StartNew();

            int tries = 0;
            while (true)
            {
                var stats = await Operations.GetStats();
                if (stats.StaleIndexes.Length != 0)
                {
                    if (tries++ % 10 == 0)
                        Operations.ShowProgress("\rWaiting {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);

                    Thread.Sleep(1000);
                    continue;
                }

                Operations.ShowProgress("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
                break;
            }

            stopwatch.Stop();
            justIndexingWait.Stop();
		}

        public virtual async Task ImportData(SmugglerImportOptions<RavenConnectionStringOptions> importOptions)
		{
            if (Options.Incremental == false)
			{
				Stream stream = importOptions.FromStream;
				bool ownStream = false;
				try
				{
					if (stream == null)
					{
						stream = File.OpenRead(importOptions.FromFile);
						ownStream = true;
				    }
					await ImportData(importOptions, stream);
				}
				finally
				{
					if (stream != null && ownStream)
						stream.Dispose();
				}
				return;
			}

			var files = Directory.GetFiles(Path.GetFullPath(importOptions.FromFile))
				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
				.OrderBy(File.GetLastWriteTimeUtc)
				.ToArray();

			if (files.Length == 0)
				return;

            var oldItemType = Options.OperateOnTypes;

            Options.OperateOnTypes = Options.OperateOnTypes & ~(ItemType.Indexes | ItemType.Transformers);

            for (var i = 0; i < files.Length - 1; i++)
            {
                using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files[i])))
                {
                    Operations.ShowProgress("Starting to import file: {0}", files[i]);
                    await ImportData(importOptions, fileStream);
                }
            }

            Options.OperateOnTypes = oldItemType;

            using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files.Last())))
            {
                Operations.ShowProgress("Starting to import file: {0}", files.Last());
                await ImportData(importOptions, fileStream);
            }
		}

        public abstract Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions);

        public async virtual Task ImportData(SmugglerImportOptions<RavenConnectionStringOptions> importOptions, Stream stream)
		{
            Operations.Configure(Options);
            Operations.Initialize(Options);
			SupportedFeatures = await DetectServerSupportedFeatures(Operations, importOptions.To);
			
			Stream sizeStream;

            var sw = Stopwatch.StartNew();
			// Try to read the stream compressed, otherwise continue uncompressed.
			JsonTextReader jsonReader;
			try
			{
				stream.Position = 0;
				sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
				var streamReader = new StreamReader(sizeStream);

				jsonReader = new RavenJsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}
			catch (Exception e)
			{
				if (e is InvalidDataException == false)
					throw;

				stream.Seek(0, SeekOrigin.Begin);

				sizeStream = new CountingStream(stream);

				var streamReader = new StreamReader(sizeStream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}

			if (jsonReader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");

			var exportCounts = new Dictionary<string, int>();
			var exportSectionRegistar = new Dictionary<string, Func<int>>();

            Options.CancelToken.Token.ThrowIfCancellationRequested();

            exportSectionRegistar.Add("Indexes", () =>
            {
                Operations.ShowProgress("Begin reading indexes");
                var indexCount = ImportIndexes(jsonReader).Result;
                Operations.ShowProgress(string.Format("Done with reading indexes, total: {0}", indexCount));
                return indexCount;
            });

			exportSectionRegistar.Add("Docs", () =>
			{
				Operations.ShowProgress("Begin reading documents");
				var documentCount = ImportDocuments(jsonReader).Result;
				Operations.ShowProgress(string.Format("Done with reading documents, total: {0}", documentCount));
				return documentCount;
			});

			exportSectionRegistar.Add("Attachments", () =>
			{
				Operations.ShowProgress("Begin reading attachments");
				var attachmentCount = ImportAttachments(importOptions.To, jsonReader).Result;
				Operations.ShowProgress(string.Format("Done with reading attachments, total: {0}", attachmentCount));
				return attachmentCount;
			});

			exportSectionRegistar.Add("Transformers", () =>
			{
				Operations.ShowProgress("Begin reading transformers");
				var transformersCount = ImportTransformers(jsonReader).Result;
				Operations.ShowProgress(string.Format("Done with reading transformers, total: {0}", transformersCount));
				return transformersCount;
			});

			exportSectionRegistar.Add("DocsDeletions", () =>
		{
				Operations.ShowProgress("Begin reading deleted documents");
				var deletedDocumentsCount = ImportDeletedDocuments(jsonReader).Result;
				Operations.ShowProgress(string.Format("Done with reading deleted documents, total: {0}", deletedDocumentsCount));
				return deletedDocumentsCount;
			});

			exportSectionRegistar.Add("AttachmentsDeletions", () =>
			{
				Operations.ShowProgress("Begin reading deleted attachments");
				var deletedAttachmentsCount = ImportDeletedAttachments(jsonReader).Result;
				Operations.ShowProgress(string.Format("Done with reading deleted attachments, total: {0}", deletedAttachmentsCount));
				return deletedAttachmentsCount;
			});

			exportSectionRegistar.Add("Identities", () =>
			{
				Operations.ShowProgress("Begin reading identities");
				var identitiesCount = ImportIdentities(jsonReader).Result;
				Operations.ShowProgress(string.Format("Done with reading identities, total: {0}", identitiesCount));
				return identitiesCount;
			});

			exportSectionRegistar.Keys.ForEach(k => exportCounts[k] = 0);

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
				Func<int> currentAction;
				var currentSection = jsonReader.Value.ToString();
				if (exportSectionRegistar.TryGetValue(currentSection, out currentAction) == false)
				{
					throw new InvalidDataException("Unexpected property found: " + jsonReader.Value);
				}
			if (jsonReader.Read() == false)
				{
					exportCounts[currentSection] = 0;
					continue;
				}

			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");

				exportCounts[currentSection] = currentAction();
			}

			sw.Stop();

			Operations.ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments, deleted {2:#,#;;0} documents and {3:#,#;;0} attachments in {4:#,#.###;;0} s", exportCounts["Docs"], exportCounts["Attachments"], exportCounts["DocsDeletions"], exportCounts["AttachmentsDeletions"], sw.ElapsedMilliseconds / 1000f);

            Options.CancelToken.Token.ThrowIfCancellationRequested();
		}

		private async Task<int> ImportIdentities(JsonTextReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				Options.CancelToken.Token.ThrowIfCancellationRequested();

				var identity = RavenJToken.ReadFrom(jsonReader);

				var identityName = identity.Value<string>("Key");

				if(FilterIdentity(identityName, Options.OperateOnTypes) == false)
					continue;

				await Operations.SeedIdentityFor(identityName, identity.Value<long>("Value"));

				count++;
			}

			await Operations.SeedIdentityFor(null, -1); // force flush

			return count;
		}

		private async Task<int> ImportDeletedDocuments(JsonReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

				var item = RavenJToken.ReadFrom(jsonReader);

				var deletedDocumentInfo = new JsonSerializer { Converters = DefaultConverters }
                                                    .Deserialize<Tombstone>(new RavenJTokenReader(item));

				Operations.ShowProgress("Importing deleted documents {0}", deletedDocumentInfo.Key);

				await Operations.DeleteDocument(deletedDocumentInfo.Key);

				count++;
			}

			return count;
		}

        [Obsolete("Use RavenFS instead.")]
		private async Task<int> ImportDeletedAttachments(JsonReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

				var item = RavenJToken.ReadFrom(jsonReader);

				var deletedAttachmentInfo = new JsonSerializer { Converters = DefaultConverters }
                                                    .Deserialize<Tombstone>(new RavenJTokenReader(item));

				Operations.ShowProgress("Importing deleted attachments {0}", deletedAttachmentInfo.Key);

				await Operations.DeleteAttachment(deletedAttachmentInfo.Key);

				count++;
			}

			return count;
		}

		private async Task<int> ImportTransformers(JsonTextReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

				var transformer = RavenJToken.ReadFrom(jsonReader);
                if ((Options.OperateOnTypes & ItemType.Transformers) != ItemType.Transformers)
					continue;

				var transformerName = transformer.Value<string>("name");

				await Operations.PutTransformer(transformerName, transformer);

				count++;
			}

			await Operations.PutTransformer(null, null); // force flush

				return count;
		}

        private static Lazy<JsonConverterCollection> defaultConverters = new Lazy<JsonConverterCollection>(() =>
        {
            var converters = new JsonConverterCollection()
            {
                new JsonToJsonConverter(),
                new StreamFromJsonConverter()
            };
            converters.Freeze();

            return converters;
        }, true);

        private static JsonConverterCollection DefaultConverters 
        {
            get { return defaultConverters.Value; }
        }

        [Obsolete("Use RavenFS instead.")]
		private async Task<int> ImportAttachments(RavenConnectionStringOptions dst, JsonTextReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

				var item = RavenJToken.ReadFrom(jsonReader);
                if ((Options.OperateOnTypes & ItemType.Attachments) != ItemType.Attachments)
					continue;

				var attachmentExportInfo = new JsonSerializer { Converters = DefaultConverters }
                                                    .Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));

				Operations.ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

				if (Options.StripReplicationInformation) 
					attachmentExportInfo.Metadata = Operations.StripReplicationInformationFromMetadata(attachmentExportInfo.Metadata);

				await Operations.PutAttachment(attachmentExportInfo);

				count++;
			}

			await Operations.PutAttachment(null); // force flush

				return count;
		}

		private async Task<int> ImportDocuments(JsonTextReader jsonReader)
		{
			var now = SystemTime.UtcNow;
			var count = 0;
            string continuationDocId = "Raven/Smuggler/Continuation/" + Options.ContinuationToken;

            var state = new OperationState
            {
                FilePath = Options.ContinuationToken,
                LastDocsEtag = Options.StartDocsEtag,
            };

            JsonDocument lastEtagsDocument = null;
            if (Options.UseContinuationFile)
            {
                lastEtagsDocument = Operations.GetDocument(continuationDocId);
                if ( lastEtagsDocument == null )
                {
                    lastEtagsDocument = new JsonDocument
                    {
                        Key = continuationDocId,                        
                        Etag = Etag.Empty,
                        DataAsJson = RavenJObject.FromObject(state)
                    };
                }
                else
                {                    
                    state = lastEtagsDocument.DataAsJson.JsonDeserialization<OperationState>();
                }

                JsonDocument.EnsureIdInMetadata(lastEtagsDocument);
            }

            int skippedDocuments = 0;
            long skippedDocumentsSize = 0;

            Etag tempLastEtag = Etag.Empty;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

				var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
                var size = DocumentHelpers.GetRoughSize(document);
				if (size > 1024 * 1024)
				{
					Console.WriteLine("Large document warning: {0:#,#.##;;0} kb - {1}",
									    (double)size / 1024,
									    document["@metadata"].Value<string>("@id"));
				}
                if ((Options.OperateOnTypes & ItemType.Documents) != ItemType.Documents)
					continue;
                if (Options.MatchFilters(document) == false)
					continue;

                if (Options.ShouldExcludeExpired && Options.ExcludeExpired(document, now))
					continue;

                if (!string.IsNullOrEmpty(Options.TransformScript))
                    document = await Operations.TransformDocument(document, Options.TransformScript);

                // If document is null after a transform we skip it. 
                if (document == null)
                    continue;

                var metadata = document["@metadata"] as RavenJObject;
                if (metadata != null)
                {
                    if (Options.SkipConflicted && metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
                        continue;

                    if (Options.StripReplicationInformation)
                        document["@metadata"] = Operations.StripReplicationInformationFromMetadata(metadata);

					if(Options.ShouldDisableVersioningBundle)
						document["@metadata"] = Operations.DisableVersioning(metadata);
                }

                if (Options.UseContinuationFile)
                {
                    tempLastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    if (tempLastEtag.CompareTo(state.LastDocsEtag) <= 0) // tempLastEtag < lastEtag therefore we are skipping.
                    {
                        skippedDocuments++;
                        skippedDocumentsSize += size;
                        continue;
                    }     
                }

				await Operations.PutDocument(document, (int)size);

				count++;

                if (count % Options.BatchSize == 0)
				{                    
                    if (Options.UseContinuationFile)
                    {                        
                        if (tempLastEtag.CompareTo(state.LastDocsEtag) > 0)
                            state.LastDocsEtag = tempLastEtag;

                        await WriteLastEtagToDatabase(state, lastEtagsDocument);
                    }

                    // Wait for the batch to be indexed before continue.
                    if (Options.WaitForIndexing)
                        await WaitForIndexingAsOfLastWrite();

                    Operations.ShowProgress("Read {0:#,#;;0} documents", count + skippedDocuments);
				}
			}            

            if (Options.UseContinuationFile)
            {
                if (tempLastEtag.CompareTo(state.LastDocsEtag) > 0)
                    state.LastDocsEtag = tempLastEtag;

                await WriteLastEtagToDatabase(state, lastEtagsDocument);

                Operations.ShowProgress("Documents skipped by continuation {0:#,#;;0} - approx. {1:#,#.##;;0} Mb.", skippedDocuments, (double)skippedDocumentsSize / 1024 / 1024);
            }

            await Operations.PutDocument(null, -1); // force flush    

			return count;
		}

        private async Task WriteLastEtagToDatabase(OperationState state, JsonDocument lastEtagsDocument)
        {
            lastEtagsDocument.DataAsJson = RavenJObject.FromObject(state);

            var stateDocument = lastEtagsDocument.ToJson();
            int stateDocumentSize = (int)DocumentHelpers.GetRoughSize(stateDocument);
            await Operations.PutDocument(stateDocument, stateDocumentSize);
        }

		private async Task<int> ImportIndexes(JsonReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
                Options.CancelToken.Token.ThrowIfCancellationRequested();

				var index = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
                if ((Options.OperateOnTypes & ItemType.Indexes) != ItemType.Indexes)
					continue;

				var indexName = index.Value<string>("name");
				if (indexName.StartsWith("Temp/"))
					continue;

				var definition = index.Value<RavenJObject>("definition");
				if (definition.Value<bool>("IsCompiled"))
					continue; // can't import compiled indexes

                if ((Options.OperateOnTypes & ItemType.RemoveAnalyzers) == ItemType.RemoveAnalyzers)
				{
					definition.Remove("Analyzers");
				}

				await Operations.PutIndex(indexName, index);

				count++;
			}

			await Operations.PutIndex(null, null);

			return count;
		}

		private async Task ExportIndexes(RavenConnectionStringOptions src, JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				var indexes = await Operations.GetIndexes(totalCount);
				if (indexes.Length == 0)
				{
					Operations.ShowProgress("Done with reading indexes, total: {0}", totalCount);
					break;
				}
				totalCount += indexes.Length;
				Operations.ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
				foreach (var index in indexes)
				{
					index.WriteTo(jsonWriter);
				}
			}
		}

		protected async Task<ServerSupportedFeatures> DetectServerSupportedFeatures(ISmugglerDatabaseOperations ops, RavenConnectionStringOptions server)
		{
			var serverVersion = await ops.GetVersion(server);
            if (string.IsNullOrEmpty(serverVersion))
            {
	            return GetLegacyModeFeatures();
            }

            var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<SmugglerDatabaseApiBase>()).ProductVersion;

            var subServerVersion = serverVersion.Substring(0, 3);
            var subSmugglerVersion = smugglerVersion.Substring(0, 3);

            var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));

			if (intServerVersion < 25)
			{
				
                ops.ShowProgress("Running in legacy mode, importing/exporting transformers is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
				return new ServerSupportedFeatures
				{
					IsTransformersSupported = false,
					IsDocsStreamingSupported = false,
					IsIdentitiesSmugglingSupported = false
				};
			}

			if (intServerVersion == 25)
			{
				ops.ShowProgress("Running in legacy mode, importing/exporting identities is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);

				return new ServerSupportedFeatures
				{
					IsTransformersSupported = true,
					IsDocsStreamingSupported = true,
					IsIdentitiesSmugglingSupported = false
				};
			}

			return new ServerSupportedFeatures
			{
				IsTransformersSupported = true,
				IsDocsStreamingSupported = true,
				IsIdentitiesSmugglingSupported = true
			};
		}

		private ServerSupportedFeatures GetLegacyModeFeatures()
		{
			var result = new ServerSupportedFeatures
			{
				IsTransformersSupported = false,
				IsDocsStreamingSupported = false,
				IsIdentitiesSmugglingSupported = false
			};
			
			Operations.ShowProgress("Server version is not available. Running in legacy mode which does not support transformers.");

			return result;
		}

		public ServerSupportedFeatures SupportedFeatures { get; private set; }

		protected static void SetDatabaseNameIfEmpty(RavenConnectionStringOptions connection)
		{
			if (string.IsNullOrWhiteSpace(connection.DefaultDatabase) == false)
				return;
			var index = connection.Url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
			if (index != -1)
			{
				connection.DefaultDatabase = connection.Url.Substring(index + "/databases/".Length).Trim(new[] { '/' });
			}
		}
	}
}
