//-----------------------------------------------------------------------
// <copyright file="SmugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Newtonsoft.Json;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
	public class SmugglerApi
	{
		public RavenConnectionStringOptions ConnectionStringOptions { get; set; }
		private readonly HttpRavenRequestFactory httpRavenRequestFactory = new HttpRavenRequestFactory();

		public SmugglerApi(RavenConnectionStringOptions connectionStringOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
		}

		private HttpRavenRequest CreateRequest(string url, string method = "GET")
		{
			return httpRavenRequestFactory.Create(ConnectionStringOptions.Url + url, method, ConnectionStringOptions);
		}

		public void ExportData(ExportSpec exportSpec)
		{
			using (var streamWriter = new StreamWriter(new GZipStream(File.Create(exportSpec.File), CompressionMode.Compress)))
			{
				var jsonWriter = new JsonTextWriter(streamWriter)
				{
					Formatting = Formatting.Indented
				};
				jsonWriter.WriteStartObject();
				jsonWriter.WritePropertyName("Indexes");
				jsonWriter.WriteStartArray();

				int totalCount = 0;
				while (true)
				{
					RavenJArray indexes = null;
					var request = CreateRequest("indexes?pageSize=128&start=" + totalCount);
					request.ExecuteRequest(reader => indexes = RavenJArray.Load(new JsonTextReader(reader)));

					if (indexes.Length == 0)
					{
						Console.WriteLine("Done with reading indexes, total: {0}", totalCount);
						break;
					}
					totalCount += indexes.Length;
					Console.WriteLine("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
					foreach (RavenJToken item in indexes)
					{
						item.WriteTo(jsonWriter);
					}
				}
				
				jsonWriter.WriteEndArray();
				jsonWriter.WritePropertyName("Docs");
				jsonWriter.WriteStartArray();

				if (!exportSpec.ExportIndexesOnly)
				{
					ExportDocuments(jsonWriter);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WritePropertyName("Attachments");
				jsonWriter.WriteStartArray();
				if (exportSpec.IncludeAttachments)
				{
					ExportAttachments(jsonWriter, exportSpec);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WriteEndObject();
				streamWriter.Flush();
			}
		}

		private void ExportDocuments(JsonTextWriter jsonWriter)
		{
			var lastEtag = Guid.Empty;
			int totalCount = 0;
			while (true)
			{
				RavenJArray documents = null;
				var request = CreateRequest("docs?pageSize=128&etag=" + lastEtag);
				request.ExecuteRequest(reader => documents = RavenJArray.Load(new JsonTextReader(reader)));

				if (documents.Length == 0)
				{
					Console.WriteLine("Done with reading documents, total: {0}", totalCount);
					break;
				}
				totalCount += documents.Length;
				Console.WriteLine("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", documents.Length,
									totalCount);
				foreach (RavenJToken item in documents)
				{
					item.WriteTo(jsonWriter);
				}
				lastEtag = new Guid(documents.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
			}
		}

		private void ExportAttachments(JsonTextWriter jsonWriter, ExportSpec exportSpec)
		{
			var lastEtag = Guid.Empty;
			int totalCount = 0;
			while (true)
			{
				RavenJArray attachmentInfo = null;
				var request = CreateRequest("static/?pageSize=128&etag=" + lastEtag);
				request.ExecuteRequest(reader => attachmentInfo = RavenJArray.Load(new JsonTextReader(reader)));

				if (attachmentInfo.Length == 0)
				{
					Console.WriteLine("Done with reading attachments, total: {0}", totalCount);
					break;
				}

				totalCount += attachmentInfo.Length;
				Console.WriteLine("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachmentInfo.Length,
				                  totalCount);
				foreach (var item in attachmentInfo)
				{
					Console.WriteLine("Downloading attachment: {0}", item.Value<string>("Key"));

					RavenJArray attachmentData = null;
					var requestData = CreateRequest("static/" + item.Value<string>("Key"));
					requestData.ExecuteRequest(reader => attachmentData = RavenJArray.Load(new JsonTextReader(reader)));

					new RavenJObject
						{
							{"Data", attachmentData},
							{"Metadata", item.Value<RavenJObject>("Metadata")},
							{"Key", item.Value<string>("Key")}
						}
						.WriteTo(jsonWriter);
				}

				lastEtag = new Guid(attachmentInfo.Last().Value<string>("Etag"));
			}
		}

		public void ImportData(string file, bool skipIndexes = false)
		{
			using (FileStream fileStream = File.OpenRead(file))
			{
				ImportData(fileStream, skipIndexes);
			}
		}

		public void ImportData(Stream stream, bool skipIndexes = false)
		{
			var sw = Stopwatch.StartNew();
			// Try to read the stream compressed, otherwise continue uncompressed.
			JsonTextReader jsonReader;
			try
			{
				var streamReader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
					return;
			}
			catch (InvalidDataException)
			{
				stream.Seek(0, SeekOrigin.Begin);

				StreamReader streamReader = new StreamReader(stream);

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
				if (skipIndexes)
					continue;
				var indexName = index.Value<string>("name");
				if (indexName.StartsWith("Raven/") || indexName.StartsWith("Temp/"))
					continue;

				var request = CreateRequest("indexes/" + indexName, "PUT");
				request.Write(index.Value<RavenJObject>("definition"));
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
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				totalCount += 1;
				var document = RavenJToken.ReadFrom(jsonReader);
				batch.Add((RavenJObject)document);
				if (batch.Count >= 128)
					FlushBatch(batch);
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

				var attachmentExportInfo =
					new JsonSerializer
					{
						Converters = { new TrivialJsonToJsonJsonConverter() }
					}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));
				Console.WriteLine("Importing attachment {0}", attachmentExportInfo.Key);

				var request = CreateRequest("bulk_docs", "POST");
				if (attachmentExportInfo.Metadata != null)
				{
					foreach (var header in attachmentExportInfo.Metadata)
					{
						request.WebRequest.Headers.Add(header.Key, StripQuotesIfNeeded(header.Value));
					}
				}

				request.Write(attachmentExportInfo.Data);
			}
			Console.WriteLine("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments in {2:#,#;;0} ms", totalCount, attachmentCount, sw.ElapsedMilliseconds);
		}

		private static string StripQuotesIfNeeded(RavenJToken value)
		{
			var str = value.ToString(Formatting.None);
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
			return str;
		}

		private void FlushBatch(List<RavenJObject> batch)
		{
			var sw = Stopwatch.StartNew();
			
			var commands = new RavenJArray();
			foreach (var doc in batch)
			{
				var metadata = doc.Value<RavenJObject>("@metadata");
				doc.Remove("@metadata");
				commands.Add(new RavenJObject
							    {
							        {"Method", "PUT"},
							        {"Document", doc},
							        {"Metadata", metadata},
							        {"Key", metadata.Value<string>("@id")}
							    });
			}
				
			var request = CreateRequest("bulk_docs", "POST");
			var size = request.Write(commands);

			Console.Write("Wrote {0} documents", batch.Count, sw.ElapsedMilliseconds);
			if (size > 0)
				Console.Write(" [{0:#,#;;0} kb]", Math.Round((double) size/1024, 2));
			Console.WriteLine(" in {0:#,#;;0} ms", sw.ElapsedMilliseconds);
			batch.Clear();
		}

		private class AttachmentExportInfo
		{
			public byte[] Data { get; set; }
			public RavenJObject Metadata { get; set; }
			public string Key { get; set; }
		}
	}
}