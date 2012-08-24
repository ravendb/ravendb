using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Json;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Smuggler
{
	public abstract class SmugglerApiBase : ISmugglerApi
	{
		private const int MaxSizeOfUncomressedSizeToSendToDatabase = 16*1024*1024;
		protected readonly SmugglerOptions smugglerOptions;
		protected abstract RavenJArray GetIndexes(int totalCount);
		protected abstract RavenJArray GetDocuments(Guid lastEtag);
		protected abstract Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag);

		protected abstract void PutIndex(string indexName, RavenJToken index);
		protected abstract void PutAttachment(AttachmentExportInfo attachmentExportInfo);
		protected abstract void FlushBatch(List<RavenJObject> batch);

		protected abstract void ShowProgress(string format, params object[] args);

		protected bool ensuredDatabaseExists;

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
			while (true)
			{
				var documents = GetDocuments(lastEtag);
				if (documents.Length == 0)
				{
					ShowProgress("Done with reading documents, total: {0}", totalCount);
					return lastEtag;
				}

				var final = documents.Where(options.MatchFilters).ToList();
				final.ForEach(item => item.WriteTo(jsonWriter));
				totalCount += final.Count;

				ShowProgress("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", documents.Length, totalCount);
				lastEtag = new Guid(documents.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
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
				if (indexName.StartsWith("Raven/") || indexName.StartsWith("Temp/"))
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
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var before = sizeStream.Position;
				var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
				var size = sizeStream.Position - before;
				if(size > 1024*1024)
				{
					Console.WriteLine("{0:#,#.##;;0} kb - {1}",
						(double)size/1024, 
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
					FlushBatch(batch);
				}
			}
			FlushBatch(batch);

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

	}
}