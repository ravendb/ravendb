using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;

#if !SILVERLIGHT
using System.IO.Compression;
#else
using Ionic.Zlib;
#endif

using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Smuggler
{
	public abstract class SmugglerApiBase : ISmugglerApi
	{
		protected readonly SmugglerOptions smugglerOptions;
		private readonly Stopwatch stopwatch = Stopwatch.StartNew();

		protected abstract RavenJArray GetIndexes(int totalCount);
		protected abstract RavenJArray GetDocuments(Guid lastEtag);
		protected abstract Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag);

		protected abstract void PutIndex(string indexName, RavenJToken index);
		protected abstract void PutAttachment(AttachmentExportInfo attachmentExportInfo);
		protected abstract void PutDocument(RavenJObject document);
		protected abstract DatabaseStatistics GetStats();

		protected abstract void ShowProgress(string format, params object[] args);

		protected bool ensuredDatabaseExists;
		private const string IncrementalExportStateFile = "IncrementalExport.state.json";

		protected double maximumBatchChangePercentage = 0.3;

		protected int minimumBatchSize = 10;
		protected int maximumBatchSize = 1024 * 4;

		protected SmugglerApiBase(SmugglerOptions smugglerOptions)
		{
			this.smugglerOptions = smugglerOptions;
		}

		public string ExportData(SmugglerOptions options, bool incremental = false)
		{
			return ExportData(options, incremental, true);
		}

		public string ExportData(SmugglerOptions options, bool incremental, bool lastEtagsFromFile)
		{
			options = options ?? smugglerOptions;
			if (options == null)
				throw new ArgumentNullException("options");

			var file = options.BackupPath;
			if (incremental)
			{
				if (Directory.Exists(options.BackupPath) == false)
				{
					if (File.Exists(options.BackupPath))
						options.BackupPath = Path.GetDirectoryName(options.BackupPath) ?? options.BackupPath;
					else
						Directory.CreateDirectory(options.BackupPath);
				}

				if (lastEtagsFromFile) ReadLastEtagsFromFile(options);

				file = Path.Combine(options.BackupPath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(file))
				{
					var counter = 1;
					while (true)
					{
						file = Path.Combine(options.BackupPath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + " - " + counter + ".ravendb-incremental-dump");

						if (File.Exists(file) == false)
							break;
						counter++;
					}
				}
			}

			using (var streamWriter = new StreamWriter(new GZipStream(File.Create(file), CompressionMode.Compress)))
			{
				var jsonWriter = new JsonTextWriter(streamWriter)
									 {
										 Formatting = Formatting.Indented
									 };
				jsonWriter.WriteStartObject();
				jsonWriter.WritePropertyName("Indexes");
				jsonWriter.WriteStartArray();
				if ((options.OperateOnTypes & ItemType.Indexes) == ItemType.Indexes)
				{
					ExportIndexes(jsonWriter);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WritePropertyName("Docs");
				jsonWriter.WriteStartArray();
				if ((options.OperateOnTypes & ItemType.Documents) == ItemType.Documents)
				{
					options.LastDocsEtag = ExportDocuments(options, jsonWriter, options.LastDocsEtag);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WritePropertyName("Attachments");
				jsonWriter.WriteStartArray();
				if ((options.OperateOnTypes & ItemType.Attachments) == ItemType.Attachments)
				{
					options.LastAttachmentEtag = ExportAttachments(jsonWriter, options.LastAttachmentEtag);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WriteEndObject();
				streamWriter.Flush();
			}

			if (incremental && lastEtagsFromFile)
				WriteLastEtagsFromFile(options);

			return file;
		}

		public static void ReadLastEtagsFromFile(SmugglerOptions options)
		{
			var etagFileLocation = Path.Combine(options.BackupPath, IncrementalExportStateFile);
			if (File.Exists(etagFileLocation))
			{
				using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
				using (var jsonReader = new JsonTextReader(streamReader))
				{
					var ravenJObject = RavenJObject.Load(jsonReader);
					options.LastDocsEtag = new Guid(ravenJObject.Value<string>("LastDocEtag"));
					options.LastAttachmentEtag = new Guid(ravenJObject.Value<string>("LastAttachmentEtag"));
				}
			}
		}

		public static void WriteLastEtagsFromFile(SmugglerOptions options)
		{
			var etagFileLocation = Path.Combine(options.BackupPath, IncrementalExportStateFile);
			using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
			{
				new RavenJObject
					{
						{"LastDocEtag", options.LastDocsEtag.ToString()},
						{"LastAttachmentEtag", options.LastAttachmentEtag.ToString()}
					}.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
			}
		}

		private Guid ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter, Guid lastEtag)
		{
			int totalCount = 0;

			while (true)
			{
				var watch = Stopwatch.StartNew();
				var documents = GetDocuments(lastEtag);
				watch.Stop();

				if (documents.Length == 0)
				{
					var databaseStatistics = GetStats();
					var lastEtagComparable = new ComparableByteArray(lastEtag);
					if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
					{
						lastEtag = Etag.Increment(lastEtag, smugglerOptions.BatchSize);
						ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
						continue;
					}
					ShowProgress("Done with reading documents, total: {0}", totalCount);
					return lastEtag;
				}

				var currentProcessingTime = watch.Elapsed;

				ModifyBatchSize(options, currentProcessingTime);

				var final = documents.Where(options.MatchFilters).ToList();

				if (options.ShouldExcludeExpired)
					final = documents.Where(options.ExcludeExpired).ToList();

				final.ForEach(item => item.WriteTo(jsonWriter));
				totalCount += final.Count;

				ShowProgress("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", documents.Length, totalCount);
				lastEtag = new Guid(documents.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
			}
		}

		public void WaitForIndexing(SmugglerOptions options)
		{
			var justIndexingWait = Stopwatch.StartNew();
			int tries = 0;
			while (true)
			{
				var databaseStatistics = GetStats();
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

		public virtual async void ImportData(SmugglerOptions options, bool incremental = false)
		{
			if (incremental == false)
			{
				using (FileStream fileStream = File.OpenRead(options.BackupPath))
				{
					await ImportData(fileStream, options);
				}

				return;
			}

			var files = Directory.GetFiles(Path.GetFullPath(options.BackupPath))
				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
				.OrderBy(File.GetLastWriteTimeUtc)
				.ToArray();

			if (files.Length == 0)
				return;

			var optionsWithoutIndexes = new SmugglerOptions
											{
												BackupPath = options.BackupPath,
												Filters = options.Filters,
												OperateOnTypes = options.OperateOnTypes & ~ItemType.Indexes
											};

			for (var i = 0; i < files.Length - 1; i++)
			{
				using (var fileStream = File.OpenRead(Path.Combine(options.BackupPath, files[i])))
				{
					await ImportData(fileStream, optionsWithoutIndexes);
				}
			}

			using (var fileStream = File.OpenRead(Path.Combine(options.BackupPath, files.Last())))
			{
				await ImportData(fileStream, options);
			}
		}

		protected class AttachmentExportInfo
		{
			public byte[] Data { get; set; }
			public RavenJObject Metadata { get; set; }
			public string Key { get; set; }
		}

		protected abstract void EnsureDatabaseExists();

		public async Task ImportData(Stream stream, SmugglerOptions options, bool importIndexes = true)
		{
			EnsureDatabaseExists();
			Stream sizeStream;

			var sw = Stopwatch.StartNew();
			// Try to read the stream compressed, otherwise continue uncompressed.
			JsonTextReader jsonReader;
			try
			{
				sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
				var streamReader = new StreamReader(sizeStream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}
			catch (InvalidDataException)
			{
				sizeStream = stream;
				stream.Seek(0, SeekOrigin.Begin);

				var streamReader = new StreamReader(stream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}

			if (jsonReader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");

			// should read indexes now
			var indexCount = await ImportIndexes(jsonReader, options);

			// should read documents now
			var documentCount = await ImportDocuments(jsonReader, sizeStream, options);

			// should read attachments now
			var attachmentCount = await ImportAttachments(jsonReader, options);

			sw.Stop();

			ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments in {2:#,#;;0} ms", documentCount, attachmentCount, sw.ElapsedMilliseconds);
		}

		private Task<int> ImportAttachments(JsonTextReader jsonReader, SmugglerOptions options)
		{
			return Task.Factory.StartNew(() =>
			{
				var count = 0;

				if (jsonReader.Read() == false || jsonReader.TokenType == JsonToken.EndObject)
					return count;
				if (jsonReader.TokenType != JsonToken.PropertyName)
					throw new InvalidDataException("PropertyName was expected");
				if (Equals("Attachments", jsonReader.Value) == false)
					throw new InvalidDataException("Attachment property was expected");
				if (jsonReader.Read() == false)
					return count;
				if (jsonReader.TokenType != JsonToken.StartArray)
					throw new InvalidDataException("StartArray was expected");
				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					var item = RavenJToken.ReadFrom(jsonReader);
					if ((options.OperateOnTypes & ItemType.Attachments) != ItemType.Attachments)
						continue;

					var attachmentExportInfo =
						new JsonSerializer
						{
							Converters =
							{
								new JsonToJsonConverter()
							}
						}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));

					ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

					PutAttachment(attachmentExportInfo);

					count++;
				}

				return count;
			});
		}

		private Task<int> ImportDocuments(JsonTextReader jsonReader, Stream sizeStream, SmugglerOptions options)
		{
			return Task.Factory.StartNew(() =>
			{
				var count = 0;

				if (jsonReader.Read() == false)
					return count;
				if (jsonReader.TokenType != JsonToken.PropertyName)
					throw new InvalidDataException("PropertyName was expected");
				if (Equals("Docs", jsonReader.Value) == false)
					throw new InvalidDataException("Docs property was expected");
				if (jsonReader.Read() == false)
					return count;
				if (jsonReader.TokenType != JsonToken.StartArray)
					throw new InvalidDataException("StartArray was expected");

				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					var before = sizeStream.Position;
					var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
					var size = sizeStream.Position - before;
					if (size > 1024 * 1024)
					{
						Console.WriteLine("{0:#,#.##;;0} kb - {1}",
										  (double)size / 1024,
										  document["@metadata"].Value<string>("@id"));
					}
					if ((options.OperateOnTypes & ItemType.Documents) != ItemType.Documents)
						continue;
					if (options.MatchFilters(document) == false)
						continue;

					PutDocument(document);

					count++;
				}

				return count;
			});
		}

		private Task<int> ImportIndexes(JsonReader jsonReader, SmugglerOptions options)
		{
			return Task.Factory.StartNew(() =>
			{
				var count = 0;

				if (jsonReader.Read() == false)
					return count;
				if (jsonReader.TokenType != JsonToken.PropertyName)
					throw new InvalidDataException("PropertyName was expected");
				if (Equals("Indexes", jsonReader.Value) == false)
					throw new InvalidDataException("Indexes property was expected");
				if (jsonReader.Read() == false)
					return count;
				if (jsonReader.TokenType != JsonToken.StartArray)
					throw new InvalidDataException("StartArray was expected");

				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					var index = RavenJToken.ReadFrom(jsonReader);
					if ((options.OperateOnTypes & ItemType.Indexes) != ItemType.Indexes)
						continue;
					var indexName = index.Value<string>("name");
					if (indexName.StartsWith("Temp/"))
						continue;
					if (index.Value<RavenJObject>("definition").Value<bool>("IsCompiled"))
						continue; // can't import compiled indexes
					PutIndex(indexName, index);

					count++;
				}

				return count;
			});
		}

		protected void ExportIndexes(JsonTextWriter jsonWriter)
		{
			int totalCount = 0;
			while (true)
			{
				RavenJArray indexes = GetIndexes(totalCount);

				if (indexes.Length == 0)
				{
					ShowProgress("Done with reading indexes, total: {0}", totalCount);
					break;
				}
				totalCount += indexes.Length;
				ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
				foreach (RavenJToken item in indexes)
				{
					item.WriteTo(jsonWriter);
				}
			}
		}

		private void ModifyBatchSize(SmugglerOptions options, TimeSpan currentProcessingTime)
		{
			var change = Math.Max(1, options.BatchSize / 3);
			int quarterTime = options.Timeout / 4;
			if (currentProcessingTime > TimeSpan.FromMilliseconds(quarterTime))
				options.BatchSize -= change;
			else
				options.BatchSize += change;

			options.BatchSize = Math.Min(maximumBatchSize, Math.Max(minimumBatchSize, options.BatchSize));
		}

	}
}
