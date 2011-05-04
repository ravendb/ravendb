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
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class Smuggler
    {
        static void Main(string[] args)
        {
            if (args.Length < 3 || args[0] != "in" && args[0] != "out")
            {
                Console.WriteLine(@"
Raven Smuggler - Import/Export utility
Usage:
    - Import the dump.raven file to a local instance:
        Raven.Smuggler in http://localhost:8080/ dump.raven
    - Export a local instance to dump.raven:
        Raven.Smuggler out http://localhost:8080/ dump.raven

      Optional arguments (after required arguments): 
            --only-indexes : exports only index definitions
            --include-attachments : also export attachments
");

                Environment.Exit(-1);
            }

            try
            {
                var instanceUrl = args[1];
                if (instanceUrl.EndsWith("/") == false)
                    instanceUrl += "/";
                var file = args[2];
                switch (args[0])
                {
                    case "in":
                        ImportData(instanceUrl, file);
                        break;
                    case "out":
                        bool exportIndexesOnly = args.Any(arg => arg.Equals("--only-indexes"));
                        bool inlcudeAttachments = args.Any(arg => arg.Equals("--include-attachments"));
                        ExportData(new ExportSpec(instanceUrl, file, exportIndexesOnly, inlcudeAttachments));
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Environment.Exit(-1);
            }
        }

        public class ExportSpec
        {
        	public ExportSpec(string instanceUrl, string file, bool exportIndexesOnly, bool includeAttachments)
            {
                InstanceUrl = instanceUrl;
                File = file;
                ExportIndexesOnly = exportIndexesOnly;
                IncludeAttachments = includeAttachments;
            }

        	public string InstanceUrl { get; private set; }

        	public string File { get; private set; }

        	public bool ExportIndexesOnly { get; private set; }

        	public bool IncludeAttachments { get; private set; }
        }

        public static void ExportData(ExportSpec exportSpec)
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
						var documents = GetString(webClient.DownloadData(exportSpec.InstanceUrl + "indexes?pageSize=128&start=" + totalCount));
                        var array = RavenJArray.Parse(documents);
                        if (array.Length == 0)
                        {
                            Console.WriteLine("Done with reading indexes, total: {0}", totalCount);
                            break;
                        }
                        totalCount += array.Length;
                        Console.WriteLine("Reading batch of {0,3} indexes, read so far: {1,10:#,#}", array.Length,
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
                if(exportSpec.IncludeAttachments)
                {
                    ExportAttachments(jsonWriter,exportSpec);
                }
                jsonWriter.WriteEndArray();

                jsonWriter.WriteEndObject();
                streamWriter.Flush();
            }

            

        }

    	private static void ExportDocuments(ExportSpec exportSpec, JsonTextWriter jsonWriter)
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
    					GetString(webClient.DownloadData(exportSpec.InstanceUrl + "docs?pageSize=128&etag=" + lastEtag));
    				var array = RavenJArray.Parse(documents);
    				if (array.Length == 0)
    				{
    					Console.WriteLine("Done with reading documents, total: {0}", totalCount);
    					break;
    				}
    				totalCount += array.Length;
    				Console.WriteLine("Reading batch of {0,3} documents, read so far: {1,10:#,#}", array.Length,
    				                  totalCount);
    				foreach (RavenJToken item in array)
    				{
    					item.WriteTo(jsonWriter);
    				}
    				lastEtag = new Guid(array.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
    			}
    		}
    	}

    	static void ExportAttachments(JsonTextWriter jsonWriter, ExportSpec exportSpec)
        {
            using(var webClient = new WebClient())
            {
            	webClient.UseDefaultCredentials = true;
            	webClient.Credentials = CredentialCache.DefaultNetworkCredentials;

				var lastEtag = Guid.Empty;
    			int totalCount = 0;
				while (true)
				{
					var attachmentInfo = GetString(webClient.DownloadData(exportSpec.InstanceUrl + "/static/?pageSize=128&etag=" + lastEtag));
					var array = RavenJArray.Parse(attachmentInfo);

					if (array.Length == 0)
					{
						Console.WriteLine("Done with reading attachments, total: {0}", totalCount);
						break;
					}

					totalCount += array.Length;
					Console.WriteLine("Reading batch of {0,3} attachments, read so far: {1,10:#,#}", array.Length,
									  totalCount);
					foreach (var item in array)
					{
						Console.WriteLine("Downloading attachment: {0}", item.Value<string>("Key"));
						var attachmentData = webClient.DownloadData(exportSpec.InstanceUrl + "/static/" + item.Value<string>("Key"));
						RavenJObject.FromObject(
							new
							{
								Data = attachmentData,
								Etag = item.Value<string>("Etag"),
								Key = item.Value<string>("Key")
							}).WriteTo(jsonWriter);
					}
				
					lastEtag = new Guid(array.Last().Value<string>("@etag"));
				}
            }
        }

        public static string GetString(byte[] downloadData)
        {
    		var ms = new MemoryStream(downloadData);
    		return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
        }

        public static void ImportData(string instanceUrl, string file)
        {
            var sw = Stopwatch.StartNew();

            using (FileStream fileStream = File.OpenRead(file))
            {
                // Try to read the stream compressed, otherwise continue uncompressed.
                JsonTextReader jsonReader;
                
                try
                {
                    StreamReader streamReader = new StreamReader(new GZipStream(fileStream, CompressionMode.Decompress));

                    jsonReader = new JsonTextReader(streamReader);
                
                    if (jsonReader.Read() == false)
                        return;
                }
                catch(InvalidDataException)
                {
                    fileStream.Seek(0, SeekOrigin.Begin);

                    StreamReader streamReader = new StreamReader(fileStream);    

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
                        var indexName = index.Value<string>("name");
                        if(indexName.StartsWith("Raven/") || indexName.StartsWith("Temp/"))
                            continue;
                        using (var streamWriter = new StreamWriter(webClient.OpenWrite(instanceUrl + "indexes/" + indexName, "PUT")))
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
                	totalCount += 1;
                    var document = RavenJToken.ReadFrom(jsonReader);
                    batch.Add((RavenJObject)document);
                    if (batch.Count >= 128)
                        FlushBatch(instanceUrl, batch);
                }
                FlushBatch(instanceUrl, batch);

                var attachmentCount = 0;
                if (jsonReader.Read() == false)
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

                        client.Headers.Add("Etag", item.Value<string>("Etag"));
                        
                        using(var writer = client.OpenWrite(instanceUrl + "static/" + item.Value<string>("Key"),"PUT"))
                        {
                            var data = item.Value<byte[]>("Data");
                            writer.Write(data,0,data.Length);
                        }
                    }
                }
           


            	Console.WriteLine("Imported {0:#,#} documents and {1:#,#} attachments in {2:#,#} ms", totalCount,attachmentCount, sw.ElapsedMilliseconds);
            }
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
        	Console.WriteLine("Wrote {0} documents [{1:#,#} kb] in {2:#,#} ms",
        	                  batch.Count, Math.Round((double)size/1024, 2), sw.ElapsedMilliseconds);
            batch.Clear();
        }
    }
}
