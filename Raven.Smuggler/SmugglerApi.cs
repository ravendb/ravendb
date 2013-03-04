//-----------------------------------------------------------------------
// <copyright file="SmugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.Imports;

namespace Raven.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		const int retriesCount = 5;

		protected override RavenJArray GetIndexes(int totalCount)
		{
			RavenJArray indexes = null;
			var request = CreateRequest("/indexes?pageSize=" + smugglerOptions.BatchSize + "&start=" + totalCount);
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

		private RemoteBulkInsertOperation bulkInsertOperation = null;

		public SmugglerApi(SmugglerOptions smugglerOptions, RavenConnectionStringOptions connectionStringOptions)
			: base(smugglerOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
		}

		public override void ImportData(SmugglerOptions options, bool incremental = false)
		{
			using (bulkInsertOperation = new RemoteBulkInsertOperation(new BulkInsertOptions(), ConnectionStringOptions, CreateRequest, text => ShowProgress(text)))
			{
				base.ImportData(options, incremental);
			}
		}

		protected override void PutDocument(RavenJObject document)
		{
			var metadata = document.Value<RavenJObject>("@metadata");
			var id = metadata.Value<string>("@id");
			document.Remove("@metadata");

			bulkInsertOperation.Write(id, metadata, document);
		}

		protected HttpRavenRequest CreateRequest(string url, string method = "GET")
		{
			var builder = new StringBuilder();
			if (url.StartsWith("http", StringComparison.InvariantCultureIgnoreCase) == false)
			{
				builder.Append(ConnectionStringOptions.Url);
				if (string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase) == false)
				{
					if (ConnectionStringOptions.Url.EndsWith("/") == false)
						builder.Append("/");
					builder.Append("databases/");
					builder.Append(ConnectionStringOptions.DefaultDatabase);
					builder.Append('/');
				}
			}
			builder.Append(url);
			var httpRavenRequest = httpRavenRequestFactory.Create(builder.ToString(), method, ConnectionStringOptions);
			httpRavenRequest.WebRequest.Timeout = smugglerOptions.Timeout;
			if (LastRequestErrored)
			{
				httpRavenRequest.WebRequest.KeepAlive = false;
				httpRavenRequest.WebRequest.Timeout *= 2;
				LastRequestErrored = false;
			}
			return httpRavenRequest;
		}

		protected override RavenJArray GetDocuments(Guid lastEtag)
		{
			int retries = retriesCount;
			while (true)
			{
				try
				{
					RavenJArray documents = null;
					var request = CreateRequest("/docs?pageSize=" + smugglerOptions.BatchSize + "&etag=" + lastEtag);
					request.ExecuteRequest(reader => documents = RavenJArray.Load(new JsonTextReader(reader)));
					return documents;
				}
				catch (Exception e)
				{
					if (retries-- == 0)
						throw;
					LastRequestErrored = true;
					ShowProgress("Error reading from database, remaining attempts {0}, will retry. Error: {1}", retries, e, retriesCount);
				}
			}
		}

		protected override Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag)
		{
			int totalCount = 0;
			while (true)
			{
				RavenJArray attachmentInfo = null;
				var request = CreateRequest("/static/?pageSize=" + smugglerOptions.BatchSize + "&etag=" + lastEtag);
				request.ExecuteRequest(reader => attachmentInfo = RavenJArray.Load(new JsonTextReader(reader)));

				if (attachmentInfo.Length == 0)
				{
					var databaseStatistics = GetStats();
					var lastEtagComparable = new ComparableByteArray(lastEtag);
					if (lastEtagComparable.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
					{
						lastEtag = Etag.Increment(lastEtag, smugglerOptions.BatchSize);
						ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}", lastEtag);
						continue;
					}
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

		protected override DatabaseStatistics GetStats()
		{
			var request = CreateRequest("/stats");
			return request.ExecuteRequest<DatabaseStatistics>();
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public bool LastRequestErrored { get; set; }

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