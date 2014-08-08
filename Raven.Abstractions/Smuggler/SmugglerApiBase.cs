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
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Smuggler
{
	public abstract class SmugglerApiBase : ISmugglerApi
	{
		private readonly Stopwatch stopwatch = Stopwatch.StartNew();

		public ISmugglerOperations Operations { get; protected set; }

		public SmugglerOptions SmugglerOptions { get; protected set; }

		private const string IncrementalExportStateFile = "IncrementalExport.state.json";

		public virtual async Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions)
		{
			var result = new ExportDataResult
			{
				FilePath = exportOptions.ToFile,
				LastAttachmentsEtag = SmugglerOptions.StartAttachmentsEtag,
				LastDocsEtag = SmugglerOptions.StartDocsEtag,
				LastDocDeleteEtag = SmugglerOptions.StartDocsDeletionEtag,
				LastAttachmentsDeleteEtag = SmugglerOptions.StartAttachmentsDeletionEtag
			};

			if (SmugglerOptions.Incremental)
			{
				if (Directory.Exists(result.FilePath) == false)
				{
					if (File.Exists(result.FilePath))
						result.FilePath = Path.GetDirectoryName(result.FilePath) ?? result.FilePath;
					else
						Directory.CreateDirectory(result.FilePath);
				}

				if (SmugglerOptions.StartDocsEtag == Etag.Empty && SmugglerOptions.StartAttachmentsEtag == Etag.Empty)
				{
					ReadLastEtagsFromFile(result);
				}

				result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(result.FilePath))
				{
					var counter = 1;
					while (true)
					{
						// ReSharper disable once AssignNullToNotNullAttribute
						result.FilePath = Path.Combine(Path.GetDirectoryName(result.FilePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + " - " + counter + ".ravendb-incremental-dump");

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
				await DetectServerSupportedFeatures(exportOptions.From);
			}
			catch (WebException e)
			{
				Operations.ShowProgress("Failed to query server for supported features. Reason : " + e.Message);
				SetLegacyMode(); //could not detect supported features, then run in legacy mode
				//				lastException = new SmugglerExportException
				//				{
				//					LastEtag = Etag.Empty,
				//					File = ownedStream ? result.FilePath : null
				//				};
			}

			try
			{
				using (var gZipStream = new GZipStream(stream, CompressionMode.Compress,
													   leaveOpen: true))
				using (var streamWriter = new StreamWriter(gZipStream))
				{
					var jsonWriter = new JsonTextWriter(streamWriter)
					{
						Formatting = Formatting.Indented
					};
					jsonWriter.WriteStartObject();
					jsonWriter.WritePropertyName("Indexes");
					jsonWriter.WriteStartArray();
					if (SmugglerOptions.OperateOnTypes.HasFlag(ItemType.Indexes))
					{
						await ExportIndexes(exportOptions.From, jsonWriter);
					}
					jsonWriter.WriteEndArray();

					// used to synchronize max returned values for put/delete operations
					var maxEtags = Operations.FetchCurrentMaxEtags();

					jsonWriter.WritePropertyName("Docs");
					jsonWriter.WriteStartArray();
					if (SmugglerOptions.OperateOnTypes.HasFlag(ItemType.Documents))
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
					if (SmugglerOptions.OperateOnTypes.HasFlag(ItemType.Attachments) && lastException == null)
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
					if (SmugglerOptions.OperateOnTypes.HasFlag(ItemType.Transformers) && lastException == null)
					{
						await ExportTransformers(exportOptions.From, jsonWriter);
					}
					jsonWriter.WriteEndArray();

					if (SmugglerOptions.ExportDeletions)
					{
						await ExportDeletions(jsonWriter, result, maxEtags);
					}

					jsonWriter.WriteEndObject();
					streamWriter.Flush();
				}

				if (SmugglerOptions.Incremental)
					WriteLastEtagsToFile(result, result.FilePath);
				if (SmugglerOptions.ExportDeletions)
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

		public static void ReadLastEtagsFromFile(ExportDataResult result)
		{
			var log = LogManager.GetCurrentClassLogger();
			var etagFileLocation = Path.Combine(result.FilePath, IncrementalExportStateFile);
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
				result.LastAttachmentsDeleteEtag =
					Etag.Parse(ravenJObject.Value<string>("LastAttachmentsDeleteEtag") ?? Etag.Empty.ToString());
			}
		}

		public static void WriteLastEtagsToFile(ExportDataResult result, string backupPath)
		{
			// ReSharper disable once AssignNullToNotNullAttribute
			var etagFileLocation = Path.Combine(Path.GetDirectoryName(backupPath), IncrementalExportStateFile);
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

		private async Task ExportTransformers(RavenConnectionStringOptions src, JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				var transformers = await Operations.GetTransformers(src, totalCount);
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

		public abstract Task ExportDeletions(JsonTextWriter jsonWriter, ExportDataResult result, LastEtagsInfo maxEtagsToFetch);

		public virtual async Task<Etag> ExportAttachments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
			var totalCount = 0;
			var maxEtagReached = false;
			while (true)
			{
				try
				{
					if (SmugglerOptions.Limit - totalCount <= 0 || maxEtagReached)
					{
						Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount);
						return lastEtag;
					}
					var maxRecords = Math.Min(SmugglerOptions.Limit - totalCount, SmugglerOptions.BatchSize);
					var array = await Operations.GetAttachments(totalCount, lastEtag, maxRecords);
					if (array.Length == 0)
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
					totalCount += array.Length;
					Operations.ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", array.Length, totalCount);
					foreach (var item in array)
					{
						var tempLastEtag = item.Value<string>("Etag");
						if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
						{
							maxEtagReached = true;
							break;
						}

						item.WriteTo(jsonWriter);
						lastEtag = tempLastEtag;
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

		protected virtual async Task<Etag> ExportDocuments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
			var now = SystemTime.UtcNow;
			var totalCount = 0;
			var lastReport = SystemTime.UtcNow;
			var reportInterval = TimeSpan.FromSeconds(2);
			var errorsCount = 0;
			var reachedMaxEtag = false;
			Operations.ShowProgress("Exporting Documents");

			while (true)
			{
				bool hasDocs = false;
				try
				{
					var maxRecords = SmugglerOptions.Limit - totalCount;
					if (maxRecords > 0 && reachedMaxEtag == false)
					{
						using (var documents = await Operations.GetDocuments(src, lastEtag, Math.Min(SmugglerOptions.BatchSize, maxRecords)))
						{
							var watch = Stopwatch.StartNew();

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

								if (!SmugglerOptions.MatchFilters(document))
									continue;

								if (SmugglerOptions.ShouldExcludeExpired && SmugglerOptions.ExcludeExpired(document, now))
									continue;

								document.WriteTo(jsonWriter);
								totalCount++;

								if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
								{
									Operations.ShowProgress("Exported {0} documents", totalCount);
									lastReport = SystemTime.UtcNow;
								}

								if (watch.ElapsedMilliseconds > 100)
									errorsCount++;

								watch.Start();
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
							lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
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
				SmugglerOptions.Filters.ForEach(filter =>
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

		public async Task WaitForIndexing()
		{
			var justIndexingWait = Stopwatch.StartNew();
			int tries = 0;
			while (true)
			{
				var databaseStatistics = await Operations.GetStats();
				if (databaseStatistics.StaleIndexes.Length != 0)
				{
					if (tries++ % 10 == 0)
					{
						Console.Write("\rWaiting {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
					}

					Thread.Sleep(1000);
					continue;
				}
				Console.WriteLine("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
				break;
			}
		}

		public virtual async Task ImportData(SmugglerImportOptions importOptions)
		{
			if (SmugglerOptions.Incremental == false)
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

			var oldItemType = SmugglerOptions.OperateOnTypes;

			SmugglerOptions.OperateOnTypes = SmugglerOptions.OperateOnTypes & ~(ItemType.Indexes | ItemType.Transformers);

			for (var i = 0; i < files.Length - 1; i++)
			{
				using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files[i])))
				{
					await ImportData(importOptions, fileStream);
				}
			}

			SmugglerOptions.OperateOnTypes = oldItemType;

			using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files.Last())))
			{
				await ImportData(importOptions, fileStream);
			}
		}

		public abstract Task Between(SmugglerBetweenOptions betweenOptions);

		public async virtual Task ImportData(SmugglerImportOptions importOptions, Stream stream)
		{
			await DetectServerSupportedFeatures(importOptions.To);

			Stream sizeStream;

			var sw = Stopwatch.StartNew();
			// Try to read the stream compressed, otherwise continue uncompressed.
			JsonTextReader jsonReader;
			try
			{
				stream.Position = 0;
				sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
				var streamReader = new StreamReader(sizeStream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}
			catch (Exception e)
			{
				if (e is InvalidDataException == false)
					throw;

				stream.Seek(0, SeekOrigin.Begin);

				sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));

				var streamReader = new StreamReader(stream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}

			if (jsonReader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");

			var exportCounts = new Dictionary<string, int>();
			var exportSectionRegistar = new Dictionary<string, Func<int>>();

			SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

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

			exportSectionRegistar.Keys.ForEach(k => exportCounts[k] = 0);

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
			{
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

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

			Operations.ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments, deleted {2:#,#;;0} documents and {3:#,#;;0} attachments in {4:#,#;;0} ms", exportCounts["Docs"], exportCounts["Attachments"], exportCounts["DocsDeletions"], exportCounts["AttachmentsDeletions"], sw.ElapsedMilliseconds);

			SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();
		}

		private async Task<int> ImportDeletedDocuments(JsonReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

				var item = RavenJToken.ReadFrom(jsonReader);

				var deletedDocumentInfo =
					new JsonSerializer
					{
						Converters =
							{
								new JsonToJsonConverter(),
                                new StreamFromJsonConverter()
							}
					}.Deserialize<Tombstone>(new RavenJTokenReader(item));

				Operations.ShowProgress("Importing deleted documents {0}", deletedDocumentInfo.Key);

				await Operations.DeleteDocument(deletedDocumentInfo.Key);

				count++;
			}

			return count;
		}

		private async Task<int> ImportDeletedAttachments(JsonReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

				var item = RavenJToken.ReadFrom(jsonReader);

				var deletedAttachmentInfo =
					new JsonSerializer
					{
						Converters =
							{
								new JsonToJsonConverter(),
                                new StreamFromJsonConverter()
							}
					}.Deserialize<Tombstone>(new RavenJTokenReader(item));

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
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

				var transformer = RavenJToken.ReadFrom(jsonReader);
				if ((SmugglerOptions.OperateOnTypes & ItemType.Transformers) != ItemType.Transformers)
					continue;

				var transformerName = transformer.Value<string>("name");

				await Operations.PutTransformer(transformerName, transformer);

				count++;
			}

			await Operations.PutTransformer(null, null); // force flush

			return count;
		}

		private async Task<int> ImportAttachments(RavenConnectionStringOptions dst, JsonTextReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

				var item = RavenJToken.ReadFrom(jsonReader);
				if ((SmugglerOptions.OperateOnTypes & ItemType.Attachments) != ItemType.Attachments)
					continue;

				var attachmentExportInfo =
					new JsonSerializer
					{
						Converters =
							{
								new JsonToJsonConverter(),
                                new StreamFromJsonConverter()
							}
					}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));

				Operations.ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

				await Operations.PutAttachment(dst, attachmentExportInfo);

				count++;
			}

			await Operations.PutAttachment(dst, null); // force flush

			return count;
		}



		private async Task<int> ImportDocuments(JsonTextReader jsonReader)
		{
			var now = SystemTime.UtcNow;
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

				var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
				var size = DocumentHelpers.GetRoughSize(document);
				if (size > 1024 * 1024)
				{
					Console.WriteLine("Large document warning: {0:#,#.##;;0} kb - {1}",
									  (double)size / 1024,
									  document["@metadata"].Value<string>("@id"));
				}
				if ((SmugglerOptions.OperateOnTypes & ItemType.Documents) != ItemType.Documents)
					continue;
				if (SmugglerOptions.MatchFilters(document) == false)
					continue;

				if (SmugglerOptions.ShouldExcludeExpired && SmugglerOptions.ExcludeExpired(document, now))
					continue;

				if (!string.IsNullOrEmpty(SmugglerOptions.TransformScript))
					document = await Operations.TransformDocument(document, SmugglerOptions.TransformScript);

				if (document == null)
					continue;

				await Operations.PutDocument(document, (int)size);

				count++;

				if (count % SmugglerOptions.BatchSize == 0)
				{
					Operations.ShowProgress("Read {0} documents", count);
				}
			}

			await Operations.PutDocument(null, -1); // force flush

			return count;
		}

		private async Task<int> ImportIndexes(JsonReader jsonReader)
		{
			var count = 0;

			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				SmugglerOptions.CancelToken.Token.ThrowIfCancellationRequested();

				var index = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
				if ((SmugglerOptions.OperateOnTypes & ItemType.Indexes) != ItemType.Indexes)
					continue;

				var indexName = index.Value<string>("name");
				if (indexName.StartsWith("Temp/"))
					continue;

				var definition = index.Value<RavenJObject>("definition");
				if (definition.Value<bool>("IsCompiled"))
					continue; // can't import compiled indexes

				if ((SmugglerOptions.OperateOnTypes & ItemType.RemoveAnalyzers) == ItemType.RemoveAnalyzers)
				{
					definition.Remove("Analyzers");
				}

				await Operations.PutIndex(indexName, index);

				count++;
			}

			await Operations.PutIndex(null, null);

			return count;
		}

		protected async Task ExportIndexes(RavenConnectionStringOptions src, JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				var indexes = await Operations.GetIndexes(src, totalCount);
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

		private async Task DetectServerSupportedFeatures(RavenConnectionStringOptions server)
		{
			var serverVersion = await Operations.GetVersion(server);
			if (string.IsNullOrEmpty(serverVersion))
			{
				SetLegacyMode();
				return;
			}

			var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<SmugglerApiBase>()).ProductVersion;

			var subServerVersion = serverVersion.Substring(0, 3);
			var subSmugglerVersion = smugglerVersion.Substring(0, 3);

			var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));

			if (intServerVersion < 25)
			{
				IsTransformersSupported = false;
				IsDocsStreamingSupported = false;
				Operations.ShowProgress("Running in legacy mode, importing/exporting transformers is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
				return;
			}

			IsTransformersSupported = true;
			IsDocsStreamingSupported = true;
		}

		private void SetLegacyMode()
		{
			IsTransformersSupported = false;
			IsDocsStreamingSupported = false;
			Operations.ShowProgress("Server version is not available. Running in legacy mode which does not support transformers.");
		}

		public bool IsTransformersSupported { get; private set; }
		public bool IsDocsStreamingSupported { get; private set; }
	}
}
