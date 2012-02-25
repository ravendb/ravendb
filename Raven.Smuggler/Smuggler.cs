//-----------------------------------------------------------------------
// <copyright file="Smuggler.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using NDesk.Options;
using Raven.Abstractions.Json;

namespace Raven.Smuggler
{
	public class Smuggler
	{       
		static void Main(string[] args)
		{
            var options = new SmugglerOptions();

            var optionSet = new OptionSet
			                	{
			                		{"metadata-filter:{=}", "Filter documents by a metadata property", (key,val) => options.Filters["@metadata." +key] = val},
									{"filter:{=}", "Filter documents by a document property", (key,val) => options.Filters[key] = val},
			                		{"only-indexes", _ => options.ExportIndexesOnly = true },
									{"include-attachments", s => options.IncludeAttachments = true }
			                	};

            // Do these arguments the traditional way to maintain compatibility
            if (String.Compare(args[0], "in", false) == 0)
                options.IsImport = true;

            if (String.Compare(args[0], "out", false) == 0)
                options.IsExport = true;

            options.InstanceUrl = args[1];
            options.File = args[2];

            try
            {
                optionSet.Parse(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                PrintUsage(optionSet);
                Environment.Exit(-1);
            }

            if (options.InstanceUrl == null | (!options.IsImport & !options.IsExport) | options.File == null)
            {
                PrintUsage(optionSet);
                Environment.Exit(-1);
            }

            if (options.InstanceUrl.EndsWith("/") == false)
                options.InstanceUrl += "/";

			try
			{
                if (options.IsImport)
                {
                    ImportData(options);
                }
                else if (options.IsExport)
                {
                    ExportData(options);
                }
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				Environment.Exit(-1);
			}
		}

		public class SmugglerOptions
		{
            public bool IsImport { get; set; }

            public bool IsExport { get; set; }

			public string InstanceUrl { get; set; }

			public string File { get; set; }

            public Dictionary<string,string> Filters { get; set; }

			public bool ExportIndexesOnly { get; set; }

			public bool IncludeAttachments { get; set; }

			public SmugglerOptions()
			{
				Filters = new Dictionary<string, string>();
			}

			public bool MatchFilters(RavenJToken item)
			{
				foreach (var filter in Filters)
				{
					var copy = filter;
					foreach (var tuple in item.SelectTokenWithRavenSyntaxReturningFlatStructure(copy.Key))
					{
						if (tuple == null || tuple.Item1 == null)
							continue;
						var val = tuple.Item1.Type == JTokenType.String
						        	? tuple.Item1.Value<string>()
						        	: tuple.Item1.ToString(Formatting.None);
						if (string.Equals(val,filter.Value, StringComparison.InvariantCultureIgnoreCase) == false)
							return false;
					}
				}
				return true;
			}
		}

		public static void ExportData(SmugglerOptions exportSpec)
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
				using (var webClient = new WebClient())
				{
					webClient.UseDefaultCredentials = true;
					webClient.Credentials = CredentialCache.DefaultNetworkCredentials;

					int totalCount = 0;
					while (true)
					{
                        var documents = GetString(webClient.DownloadData(String.Format("{0}indexes?pageSize=128&start={1}", exportSpec.InstanceUrl, totalCount)));
						var array = RavenJArray.Parse(documents);
						if (array.Length == 0)
						{
							Console.WriteLine("Done with reading indexes, total: {0}", totalCount);
							break;
						}
						totalCount += array.Length;
						Console.WriteLine("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", array.Length,
										  totalCount);
						foreach (RavenJToken item in array)
						{
							item.WriteTo(jsonWriter);
						}
					}
				}
				jsonWriter.WriteEndArray();
				jsonWriter.WritePropertyName("Docs");
				jsonWriter.WriteStartArray();

				if (!exportSpec.ExportIndexesOnly)
				{
					ExportDocuments(exportSpec, jsonWriter);
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

		private static void ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter)
		{
			using (var webClient = new WebClient())
			{
				webClient.UseDefaultCredentials = true;
				webClient.Credentials = CredentialCache.DefaultNetworkCredentials;

				var lastEtag = Guid.Empty;
				int totalCount = 0;
				while (true)
				{
					var documents =
                        GetString(webClient.DownloadData(String.Format("{0}docs?pageSize=128&etag={1}", options.InstanceUrl, lastEtag)));
					var array = RavenJArray.Parse(documents);
					if (array.Length == 0)
					{
						Console.WriteLine("Done with reading documents, total: {0}", totalCount);
						break;
					}

					var final = array.Where(options.MatchFilters).ToList();
					final.ForEach(item => item.WriteTo(jsonWriter));
					totalCount += final.Count;

					Console.WriteLine("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", array.Length,
									  totalCount);


					lastEtag = new Guid(array.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
				}
			}
		}

		static void ExportAttachments(JsonTextWriter jsonWriter, SmugglerOptions exportSpec)
		{
			using (var webClient = new WebClient())
			{
				webClient.UseDefaultCredentials = true;
				webClient.Credentials = CredentialCache.DefaultNetworkCredentials;

				var lastEtag = Guid.Empty;
				int totalCount = 0;
				while (true)
				{
                    var attachmentInfo = GetString(webClient.DownloadData(String.Format("{0}/static/?pageSize=128&etag={1}", exportSpec.InstanceUrl, lastEtag)));
					var array = RavenJArray.Parse(attachmentInfo);

					if (array.Length == 0)
					{
						Console.WriteLine("Done with reading attachments, total: {0}", totalCount);
						break;
					}

					totalCount += array.Length;
					Console.WriteLine("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", array.Length,
									  totalCount);
					foreach (var item in array)
					{
						Console.WriteLine("Downloading attachment: {0}", item.Value<string>("Key"));
                        var attachmentData = webClient.DownloadData(String.Format("{0}/static/{1}", exportSpec.InstanceUrl, item.Value<string>("Key")));

						new RavenJObject
						{
							{"Data", attachmentData},
							{"Metadata", item.Value<RavenJObject>("Metadata")},
							{"Key", item.Value<string>("Key")}
						}
						.WriteTo(jsonWriter);
					}

					lastEtag = new Guid(array.Last().Value<string>("Etag"));
				}
			}
		}

		private class AttachmentExportInfo
		{
			public byte[] Data { get; set; }
			public RavenJObject Metadata { get; set; }
			public string Key { get; set; }
		}

		public static string GetString(byte[] downloadData)
		{
			var ms = new MemoryStream(downloadData);
			return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
		}

        public static void ImportData(SmugglerOptions options)
		{
			using (FileStream fileStream = File.OpenRead(options.File))
			{
                ImportData(fileStream, options);
			}
		}

        public static void ImportData(Stream stream, SmugglerOptions options)
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
        	using (var webClient = new WebClient())
        	{
        		webClient.UseDefaultCredentials = true;
        		webClient.Headers.Add("Content-Type", "application/json; charset=utf-8");
        		webClient.Credentials = CredentialCache.DefaultNetworkCredentials;
        		while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
        		{
        			var index = RavenJToken.ReadFrom(jsonReader);
        			if (options.ExportIndexesOnly)
        				continue;
        			var indexName = index.Value<string>("name");
        			if (indexName.StartsWith("Raven/") || indexName.StartsWith("Temp/"))
        				continue;
        			using (
        				var streamWriter =
        					new StreamWriter(webClient.OpenWrite(String.Format("{0}indexes/{1}", options.InstanceUrl, indexName), "PUT"))
        				)
        			using (var jsonTextWriter = new JsonTextWriter(streamWriter))
        			{
        				index.Value<RavenJObject>("definition").WriteTo(jsonTextWriter);
        				jsonTextWriter.Flush();
        				streamWriter.Flush();
        			}
        		}
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
        		var document = (RavenJObject) RavenJToken.ReadFrom(jsonReader);

				if(options.MatchFilters(document) == false)
					continue;

				totalCount += 1;

        		batch.Add(document);
        		if (batch.Count >= 128)
        			FlushBatch(options.InstanceUrl, batch);
        	}
        	FlushBatch(options.InstanceUrl, batch);

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
        		using (var client = new WebClient())
        		{
        			attachmentCount += 1;
        			var item = RavenJToken.ReadFrom(jsonReader);

        			var attachmentExportInfo =
        				new JsonSerializer
        				{
        					Converters = {new TrivialJsonToJsonJsonConverter()}
        				}.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));
        			Console.WriteLine("Importing attachment {0}", attachmentExportInfo.Key);
        			if (attachmentExportInfo.Metadata != null)
        			{
        				foreach (var header in attachmentExportInfo.Metadata)
        				{
        					client.Headers.Add(header.Key, StripQuotesIfNeeded(header.Value));
        				}
        			}

        			using (
        				var writer = client.OpenWrite(
        					String.Format("{0}static/{1}", options.InstanceUrl, attachmentExportInfo.Key), "PUT"))
        			{
        				writer.Write(attachmentExportInfo.Data, 0, attachmentExportInfo.Data.Length);
        				writer.Flush();
        			}
        		}
        	}
        	Console.WriteLine("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments in {2:#,#;;0} ms", totalCount,
        	                  attachmentCount, sw.ElapsedMilliseconds);
        }

		public class TrivialJsonToJsonJsonConverter : JsonConverter
		{
			public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
			{
				throw new NotImplementedException();
			}

			public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
			{
				return RavenJObject.Load(reader);
			}

			public override bool CanConvert(Type objectType)
			{
				return objectType == typeof (RavenJObject);
			}
		}

		private static string StripQuotesIfNeeded(RavenJToken value)
		{
			var str = value.ToString(Formatting.None);
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
			return str;
		}

		private static void FlushBatch(string instanceUrl, List<RavenJObject> batch)
		{
			var sw = Stopwatch.StartNew();
			long size;
			using (var webClient = new WebClient())
			{
				webClient.Headers.Add("Content-Type", "application/json; charset=utf-8");
				webClient.UseDefaultCredentials = true;
				webClient.Credentials = CredentialCache.DefaultNetworkCredentials;
				using (var stream = new MemoryStream())
				{
					using (var streamWriter = new StreamWriter(stream, Encoding.UTF8))
					using (var jsonTextWriter = new JsonTextWriter(streamWriter))
					{
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
						commands.WriteTo(jsonTextWriter);
						jsonTextWriter.Flush();
						streamWriter.Flush();
						stream.Flush();
						size = stream.Length;

						using (var netStream = webClient.OpenWrite(instanceUrl + "bulk_docs", "POST"))
						{
							stream.WriteTo(netStream);
							netStream.Flush();
						}
					}
				}

			}
			Console.WriteLine("Wrote {0} documents [{1:#,#;;0} kb] in {2:#,#;;0} ms",
							  batch.Count, Math.Round((double)size / 1024, 2), sw.ElapsedMilliseconds);
			batch.Clear();
		}

        private static void PrintUsage(OptionSet optionSet)
        {
            Console.WriteLine(
                @"
Smuggler Import/Export utility for RavenDB
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Usage:
	- Import the dump.raven file to a local instance:
		Raven.Smuggler in http://localhost:8080/ dump.raven
	- Export a local instance to dump.raven:
		Raven.Smuggler out http://localhost:8080/ dump.raven
    - Export a filtered local instance to dump.raven
        Raven.Smuggler out http://localhost:8080/ dump.raven --metadata-filter:Raven-Entity-Name=Birds

Command line options:", DateTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);

            Console.WriteLine();
        }
	}
}
