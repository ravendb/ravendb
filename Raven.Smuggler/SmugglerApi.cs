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
using System.Text;
using Newtonsoft.Json;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
	public class SmugglerApi
	{
		public RavenConnectionStringOptions ConnectionStringOptions { get; private set; }
		private readonly HttpRavenRequestFactory httpRavenRequestFactory = new HttpRavenRequestFactory();

		public SmugglerApi(RavenConnectionStringOptions connectionStringOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
		}

		private HttpRavenRequest CreateRequest(string url, string method = "GET")
		{
			var builder = new StringBuilder(ConnectionStringOptions.Url, 2);
			if (string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase) == false)
			{
				builder.Append(ConnectionStringOptions.DefaultDatabase);
				builder.Append('/');
			}
			builder.Append(url);
			return httpRavenRequestFactory.Create(builder.ToString(), method, ConnectionStringOptions);
		}

		public void ExportData(SmugglerOptions options)
		{
			using (var streamWriter = new StreamWriter(new GZipStream(File.Create(options.File), CompressionMode.Compress)))
			{
				var jsonWriter = new JsonTextWriter(streamWriter)
				{
					Formatting = Formatting.Indented
				};
				jsonWriter.WriteStartObject();
				jsonWriter.WritePropertyName("Indexes");
				jsonWriter.WriteStartArray();
				if (options.OperateOnTypes.HasFlag(ItemType.Indexes))
				{
					ExportIndexes(jsonWriter);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WritePropertyName("Docs");
				jsonWriter.WriteStartArray();
				if (options.OperateOnTypes.HasFlag(ItemType.Documents))
				{
					ExportDocuments(options, jsonWriter);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WritePropertyName("Attachments");
				jsonWriter.WriteStartArray();
				if (options.OperateOnTypes.HasFlag(ItemType.Attachments))
				{
					ExportAttachments(jsonWriter);
				}
				jsonWriter.WriteEndArray();

				jsonWriter.WriteEndObject();
				streamWriter.Flush();
			}
		}

		private void ExportIndexes(JsonTextWriter jsonWriter)
		{
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
		}

		private void ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter)
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

				var final = documents.Where(options.MatchFilters).ToList();
				final.ForEach(item => item.WriteTo(jsonWriter));
				totalCount += final.Count;

				Console.WriteLine("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", documents.Length, totalCount);
				lastEtag = new Guid(documents.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
			}
		}

		private void ExportAttachments(JsonTextWriter jsonWriter)
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
				Console.WriteLine("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachmentInfo.Length, totalCount);
				foreach (var item in attachmentInfo)
				{
					Console.WriteLine("Downloading attachment: {0}", item.Value<string>("Key"));

					byte[] attachmentData = null;
					var requestData = CreateRequest("static/" + item.Value<string>("Key"));
					requestData.ExecuteRequest(reader => attachmentData = reader.ReadData());

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

		public void ImportData(SmugglerOptions options)
		{
			using (FileStream fileStream = File.OpenRead(options.File))
			{
				ImportData(fileStream, options);
			}
		}

		public void ImportData(Stream stream, SmugglerOptions options)
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
				if (options.OperateOnTypes.HasFlag(ItemType.Indexes) == false)
					continue;
				var indexName = index.Value<string>("name");
				if (indexName.StartsWith("Raven/") || indexName.StartsWith("Temp/"))
					continue;

				var request = CreateRequest("indexes/" + indexName, "PUT");
				request.Write(index.Value<RavenJObject>("definition"));
				request.ExecuteRequest();
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
				var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
				if (options.OperateOnTypes.HasFlag(ItemType.Documents) == false)
					continue;
				if (options.MatchFilters(document) == false)
					continue;

				totalCount += 1;
				batch.Add(document);
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
				if (options.OperateOnTypes.HasFlag(ItemType.Attachments) == false)
					continue;
				var attachmentExportInfo =
					new JsonSerializer
					{
						Converters = { new TrivialJsonToJsonJsonConverter() }
					}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));
				Console.WriteLine("Importing attachment {0}", attachmentExportInfo.Key);

				var request = CreateRequest("static/" + attachmentExportInfo.Key, "PUT");
				if (attachmentExportInfo.Metadata != null)
				{
					foreach (var header in attachmentExportInfo.Metadata)
					{
						request.WebRequest.Headers.Add(header.Key, StripQuotesIfNeeded(header.Value));
					}
				}

				request.Write(attachmentExportInfo.Data);
				request.ExecuteRequest();
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
			request.ExecuteRequest();

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