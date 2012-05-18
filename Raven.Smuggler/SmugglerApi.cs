//-----------------------------------------------------------------------
// <copyright file="SmugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		protected override RavenJArray GetIndexes(int totalCount)
		{
			RavenJArray indexes = null;
			var request = CreateRequest("/indexes?pageSize=128&start=" + totalCount);
			request.ExecuteRequest(reader => indexes = RavenJArray.Load(new JsonTextReader(reader)));
			return indexes;
		}

		private static string StripQuotesIfNeeded(RavenJToken value)
		{
			var str = value.ToString(Formatting.None);
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
			return str;
		}
		public RavenConnectionStringOptions ConnectionStringOptions { get; private set; }
		private readonly HttpRavenRequestFactory httpRavenRequestFactory = new HttpRavenRequestFactory();

		public SmugglerApi(RavenConnectionStringOptions connectionStringOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
		}

		protected HttpRavenRequest CreateRequest(string url, string method = "GET")
		{
			var builder = new StringBuilder(ConnectionStringOptions.Url, 2);
			if (string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase) == false)
			{
				if (ConnectionStringOptions.Url.EndsWith("/") == false)
					builder.Append("/");
				builder.Append("databases/");
				builder.Append(ConnectionStringOptions.DefaultDatabase);
				builder.Append('/');
			}
			builder.Append(url);
			return httpRavenRequestFactory.Create(builder.ToString(), method, ConnectionStringOptions);
		}

		protected override RavenJArray GetDocuments(Guid lastEtag)
		{
			RavenJArray documents = null;
			var request = CreateRequest("/docs?pageSize=128&etag=" + lastEtag);
			request.ExecuteRequest(reader => documents = RavenJArray.Load(new JsonTextReader(reader)));
			return documents;
		}

		protected override Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag)
		{
			int totalCount = 0;
			while (true)
			{
				RavenJArray attachmentInfo = null;
				var request = CreateRequest("/static/?pageSize=128&etag=" + lastEtag);
				request.ExecuteRequest(reader => attachmentInfo = RavenJArray.Load(new JsonTextReader(reader)));

				if (attachmentInfo.Length == 0)
				{
					ShowProgress("Done with reading attachments, total: {0}", totalCount);
					return lastEtag;
				}

				totalCount += attachmentInfo.Length;
				ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachmentInfo.Length, totalCount);
				foreach (var item in attachmentInfo)
				{
					ShowProgress("Downloading attachment: {0}", item.Value<string>("Key"));

					byte[] attachmentData = null;
					var requestData = CreateRequest("/static/" + item.Value<string>("Key"));
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

		protected override void PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			var request = CreateRequest("/static/" + attachmentExportInfo.Key, "PUT");
			if (attachmentExportInfo.Metadata != null)
			{
				foreach (var header in attachmentExportInfo.Metadata)
				{
					switch (header.Key)
					{
						case "Content-Type":
							request.WebRequest.ContentType = header.Value.Value<string>();
							break;
						default:
							request.WebRequest.Headers.Add(header.Key, StripQuotesIfNeeded(header.Value));
							break;
					}
				}
			}

			request.Write(attachmentExportInfo.Data);
			request.ExecuteRequest();

		}

		protected override void PutIndex(string indexName, RavenJToken index)
		{
			var request = CreateRequest("/indexes/" + indexName, "PUT");
			request.Write(index.Value<RavenJObject>("definition"));
			request.ExecuteRequest();
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		protected override void FlushBatch(List<RavenJObject> batch)
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

			var request = CreateRequest("/bulk_docs", "POST");
			request.Write(commands);
			request.ExecuteRequest();

			ShowProgress("Wrote {0} documents in {1}", batch.Count, sw.ElapsedMilliseconds);

			ShowProgress(" in {0:#,#;;0} ms", sw.ElapsedMilliseconds);
			batch.Clear();
		}

		protected override void EnsureDatabaseExists()
		{
			if (ensuredDatabaseExists ||
				string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase))
				return;

			ensuredDatabaseExists = true;

			var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(ConnectionStringOptions.Url);
			var docUrl = rootDatabaseUrl + "/docs/Raven/Databases/" + ConnectionStringOptions.DefaultDatabase;

			try
			{
				httpRavenRequestFactory.Create(docUrl, "GET", ConnectionStringOptions).ExecuteRequest();
				return;
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = CreateRequest(docUrl, "PUT");
			var document = MultiDatabase.CreateDatabaseDocument(ConnectionStringOptions.DefaultDatabase);
			request.Write(document);
			request.ExecuteRequest();
		}
	}
}