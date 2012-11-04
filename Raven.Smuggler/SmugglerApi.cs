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
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		const int retriesCount = 5;

		private int total;
		private int count;
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

		public SmugglerApi(SmugglerOptions smugglerOptions, RavenConnectionStringOptions connectionStringOptions)
			: base(smugglerOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
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

		protected override Guid FlushBatch(List<RavenJObject> batch)
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

			var retries = retriesCount;
			HttpRavenRequest request = null;
			BatchResult[] results;
			while (true)
			{
				try
				{
					request = CreateRequest("/bulk_docs", "POST");
					request.Write(commands);
					results = request.ExecuteRequest<BatchResult[]>();
					sw.Stop();
					break;
				}
				catch (Exception e)
				{
					if (--retries == 0 || request == null)
						throw;
					sw.Stop();
					LastRequestErrored = true;
					ShowProgress("Error flushing to database, remaining attempts {0} - time {2:#,#} ms, will retry [{3:#,#.##;;0} kb compressed to {4:#,#.##;;0} kb]. Error: {1}",
								 retriesCount - retries, e, sw.ElapsedMilliseconds,
								 (double)request.NumberOfBytesWrittenUncompressed / 1024,
								 (double)request.NumberOfBytesWrittenCompressed / 1024);
				}
			}
			total += batch.Count;
			ShowProgress("{2,5:#,#}: Wrote {0:#,#;;0} in {1,6:#,#;;0} ms ({6:0.00} ms per doc) (total of {3:#,#;;0}) documents [{4:#,#.##;;0} kb compressed to {5:#,#.##;;0} kb]",
				batch.Count, sw.ElapsedMilliseconds, ++count, total,
				(double)request.NumberOfBytesWrittenUncompressed / 1024,
				(double)request.NumberOfBytesWrittenCompressed / 1024,
				Math.Round((double)sw.ElapsedMilliseconds / Math.Max(1, batch.Count), 2));

			batch.Clear();

			if (results.Length == 0)
				return Guid.Empty;
			return results.Last().Etag.Value;
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