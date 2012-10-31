using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Smuggler
{
	public abstract class SmugglerApiBase : ISmugglerApi
	{
		private const int MaxSizeOfUncomressedSizeToSendToDatabase = 16 * 1024 * 1024;
		protected readonly SmugglerOptions smugglerOptions;
		private readonly Stopwatch stopwatch = Stopwatch.StartNew();
		private readonly LinkedList<Tuple<Guid, DateTime>> batchRecording = new LinkedList<Tuple<Guid, DateTime>>();

		protected abstract RavenJArray GetIndexes(int totalCount);
		protected abstract RavenJArray GetDocuments(Guid lastEtag);
		protected abstract Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag);

		protected abstract void PutIndex(string indexName, RavenJToken index);
		protected abstract void PutAttachment(AttachmentExportInfo attachmentExportInfo);
		protected abstract Guid FlushBatch(List<RavenJObject> batch);
		protected abstract DatabaseStatistics GetStats();

		protected abstract void ShowProgress(string format, params object[] args);

		protected bool ensuredDatabaseExists;

		protected double maximumBatchChangePercentage = 0.3;

		protected int minimumBatchSize = 10;

		public SmugglerApiBase(SmugglerOptions smugglerOptions)
		{
			this.smugglerOptions = smugglerOptions;
		}

		public void ExportData(SmugglerOptions options, bool incremental = false)
		{
			var lastDocsEtag = Guid.Empty;
			var lastAttachmentEtag = Guid.Empty;
			var folder = options.File;
			var etagFileLocation = Path.Combine(folder, "IncrementalExport.state.json");
			if (incremental == true)
			{
				if (Directory.Exists(folder) == false)
				{
					Directory.CreateDirectory(folder);
				}

				options.File = Path.Combine(folder, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(options.File))
				{
					var counter = 1;
					var found = false;
					while (found == false)
					{
						options.File = Path.Combine(folder, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + " - " + counter + ".ravendb-incremental-dump");

						if (File.Exists(options.File) == false)
							found = true;
						counter++;
					}
				}

				if (File.Exists(etagFileLocation))
				{
					using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
					using (var jsonReader = new JsonTextReader(streamReader))
					{
						var ravenJObject = RavenJObject.Load(jsonReader);
						lastDocsEtag = new Guid(ravenJObject.Value<string>("LastDocEtag"));
						lastAttachmentEtag = new Guid(ravenJObject.Value<string>("LastAttachmentEtag"));
					}
				}
			}


			using (var streamWriter = new StreamWriter(new GZipStream(File.Create(options.File), CompressionMode.Compress)))
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
					lastDocsEtag = ExportDocuments(options, jsonWriter, lastDocsEtag);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WritePropertyName("Attachments");
				jsonWriter.WriteStartArray();
				if ((options.OperateOnTypes & ItemType.Attachments) == ItemType.Attachments)
				{
					lastAttachmentEtag = ExportAttachments(jsonWriter, lastAttachmentEtag);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WriteEndObject();
				streamWriter.Flush();
			}
			if (incremental != true)
				return;

			using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
			{
				new RavenJObject
					{
						{"LastDocEtag", lastDocsEtag.ToString()},
						{"LastAttachmentEtag", lastAttachmentEtag.ToString()}
					}.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
			}
		}

		private Guid ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter, Guid lastEtag)
		{
			int totalCount = 0;

			long previousProcessingTime = 0;

			while (true)
			{
				var watch = Stopwatch.StartNew();
				var documents = GetDocuments(lastEtag);
				watch.Stop();

				if (documents.Length == 0)
				{
					ShowProgress("Done with reading documents, total: {0}", totalCount);
					return lastEtag;
				}

				var currentProcessingTime = watch.ElapsedTicks;

				if (previousProcessingTime == 0)
					previousProcessingTime = watch.ElapsedTicks;

				options.BatchSize = CalculateBatchSize(options.BatchSize, previousProcessingTime, currentProcessingTime);

				previousProcessingTime = currentProcessingTime;

				var final = documents.Where(options.MatchFilters).ToList();
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

					Thread.Sleep(100);
					continue;
				}
				Console.WriteLine("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
				break;
			}
		}

		public void ImportData(SmugglerOptions options, bool incremental = false)
		{
			if (incremental == false)
			{
				using (FileStream fileStream = File.OpenRead(options.File))
				{
					ImportData(fileStream, options);
				}
				return;
			}

			var files = Directory.GetFiles(Path.GetFullPath(options.File))
				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
				.OrderBy(File.GetLastWriteTimeUtc)
				.ToArray();

			if (files.Length == 0)
				return;

			var optionsWithoutIndexes = new SmugglerOptions
											{
												File = options.File,
												Filters = options.Filters,
												OperateOnTypes = options.OperateOnTypes & ~ItemType.Indexes
											};

			for (var i = 0; i < files.Length - 1; i++)
			{
				using (var fileStream = File.OpenRead(Path.Combine(options.File, files[i])))
				{
					ImportData(fileStream, optionsWithoutIndexes);
				}
			}

			using (var fileStream = File.OpenRead(Path.Combine(options.File, files.Last())))
			{
				ImportData(fileStream, options);
			}
		}

		protected class AttachmentExportInfo
		{
			public byte[] Data { get; set; }
			public RavenJObject Metadata { get; set; }
			public string Key { get; set; }
		}

		protected abstract void EnsureDatabaseExists();

		public void ImportData(Stream stream, SmugglerOptions options, bool importIndexes = true)
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
			if (jsonReader.Read() == false)
				return;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Indexes", jsonReader.Value) == false)
				throw new InvalidDataException("Indexes property was expected");
			if (jsonReader.Read() == false)
				return;
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
			}

			// should read documents now
			if (jsonReader.Read() == false)
				return;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Docs", jsonReader.Value) == false)
				throw new InvalidDataException("Docs property was expected");
			if (jsonReader.Read() == false)
				return;
			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");
			var batch = new List<RavenJObject>();
			int totalCount = 0;
			long lastFlushedAt = 0;
			int batchCount = 0;
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

				totalCount += 1;
				batch.Add(document);
				var sizeOnDisk = (sizeStream.Position - lastFlushedAt);
				if (batch.Count >= smugglerOptions.BatchSize ||
					sizeOnDisk >= MaxSizeOfUncomressedSizeToSendToDatabase)
				{
					lastFlushedAt = sizeStream.Position;
					RegisterEtagPut(FlushBatch(batch));
					if (batchCount++ % 10 == 0)
					{
						OutputIndexingDistance();
					}
				}
			}
			RegisterEtagPut(FlushBatch(batch));
			OutputIndexingDistance();

			var attachmentCount = 0;
			if (jsonReader.Read() == false || jsonReader.TokenType == JsonToken.EndObject)
				return;
			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");
			if (Equals("Attachments", jsonReader.Value) == false)
				throw new InvalidDataException("Attachment property was expected");
			if (jsonReader.Read() == false)
				return;
			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				attachmentCount += 1;
				var item = RavenJToken.ReadFrom(jsonReader);
				if ((options.OperateOnTypes & ItemType.Attachments) != ItemType.Attachments)
					continue;
				var attachmentExportInfo =
					new JsonSerializer
						{
							Converters = { new JsonToJsonConverter() }
						}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));
				ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

				PutAttachment(attachmentExportInfo);
			}
			ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments in {2:#,#;;0} ms", totalCount, attachmentCount, sw.ElapsedMilliseconds);
		}

		private void RegisterEtagPut(Guid lastEtagInBatch)
		{
			batchRecording.AddLast(Tuple.Create(lastEtagInBatch, SystemTime.UtcNow));
		}

		private void OutputIndexingDistance()
		{
			var databaseStatistics = GetStats();
			var earliestIndexedEtag = Guid.Empty;
			foreach (var indexStat in databaseStatistics.Indexes)
			{
				if (earliestIndexedEtag.CompareTo(indexStat.LastIndexedEtag) < 0)
				{
					earliestIndexedEtag = indexStat.LastIndexedEtag;
				}
			}

			var latest = DateTime.MinValue;
			var node = batchRecording.Last;
			while (node != null)
			{
				if (earliestIndexedEtag.CompareTo(node.Value.Item1) >= 0)
				{
					latest = node.Value.Item2;
					break;
				}

				node = node.Previous;
			}


			var currentDoc = BitConverter.ToInt64(databaseStatistics.LastDocEtag.ToByteArray().Reverse().ToArray(), 0);
			var lastIndexed = BitConverter.ToInt64(earliestIndexedEtag.ToByteArray().Reverse().ToArray(), 0);

			var distance = Math.Max(0, currentDoc - lastIndexed);
			TimeSpan latency = TimeSpan.Zero;
			if (latest != DateTime.MinValue)
			{
				latency = SystemTime.UtcNow - latest;
			}

			Console.WriteLine("{0} indexes, distance: {1:#,#} - latency: {2} - batch: {3:#,#}", databaseStatistics.Indexes.Length, distance,
				ToHumanTimeSpan(latency),
				databaseStatistics.CurrentNumberOfItemsToIndexInSingleBatch);
		}

		private static string ToHumanTimeSpan(TimeSpan timeAgo)
		{
			if (timeAgo == TimeSpan.Zero)
				return "zero";

			if (timeAgo.TotalDays >= 1)
				return string.Format("{0:#,#} days ago", timeAgo.TotalDays);
			if (timeAgo.TotalHours >= 1)
				return string.Format("{0:#,#} hours ago", timeAgo.TotalHours);
			if (timeAgo.TotalMinutes >= 1)
				return string.Format("{0:#,#} minutes ago", timeAgo.TotalMinutes);
			if (timeAgo.TotalSeconds >= 1)
				return string.Format("{0:#,#} seconds ago", timeAgo.TotalSeconds);

			return string.Format("{0:#,#} milli-seconds ago", timeAgo.TotalMilliseconds);
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

		private int CalculateBatchSize(int currentBatchSize, long previousProcessingTime, long currentProcessingTime)
		{
			var processingTimeDifference = (previousProcessingTime - currentProcessingTime) / (double)Math.Max(previousProcessingTime, currentProcessingTime);

			processingTimeDifference = Math.Min(maximumBatchChangePercentage, Math.Abs(processingTimeDifference))
			                           * Math.Sign(processingTimeDifference);

			return Math.Max(minimumBatchSize, currentBatchSize + (int)(processingTimeDifference * currentBatchSize));
		}

	}
}