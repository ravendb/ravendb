using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;

namespace Raven.Smuggler.Imports
{
	public interface ILowLevelBulkInsertOperation : IDisposable
	{
		void Write(string id, RavenJObject metadata, RavenJObject data);

		/// <summary>
		///     Report on the progress of the operation
		/// </summary>
		event Action<string> Report;
	}

	public class RemoteBulkInsertOperation : ILowLevelBulkInsertOperation
	{
		private readonly BulkInsertOptions options;
		private readonly RavenConnectionStringOptions connectionOptions;

		private readonly Func<string, string, HttpRavenRequest> createRequest;

		private readonly MemoryStream bufferedStream = new MemoryStream();
		private readonly HttpRavenRequest httpRavenRequest;
		private readonly BlockingCollection<RavenJObject> items;
		private readonly Task nextTask;
		private int total;

		public RemoteBulkInsertOperation(BulkInsertOptions options, RavenConnectionStringOptions connectionOptions, Func<string, string, HttpRavenRequest> createRequest, Action<string> report = null)
		{
			this.options = options;
			this.connectionOptions = connectionOptions;

			this.createRequest = createRequest;

			if (report != null)
			{
				this.Report += report;
			}

			items = new BlockingCollection<RavenJObject>(options.BatchSize * 8);

			string requestUrl = "/bulkInsert?";
			if (options.CheckForUpdates)
				requestUrl += "checkForUpdates=true";
			if (options.CheckReferencesInIndexes)
				requestUrl += "&checkReferencesInIndexes=true";

			var expect100Continue = Expect100Continue();

			// this will force the HTTP layer to authenticate, meaning that our next request won't have to
			CreateBulkRequest(requestUrl + "&no-op=for-auth-only", "POST")
				.ExecuteRequest();

			httpRavenRequest = CreateBulkRequest(requestUrl, "POST");

			nextTask = GetRawRequestStream(httpRavenRequest)
				.ContinueWith(task =>
				{
					try
					{
						expect100Continue.Dispose();
					}
					catch (Exception) { }

					WriteQueueToServer(task);
				});
		}

		private void WriteQueueToServer(Task<Stream> task)
		{
			Stream requestStream = task.Result;
			while (true)
			{
				var batch = new List<RavenJObject>();
				RavenJObject item;
				while (items.TryTake(out item, 200))
				{
					if (item == null) // marker
					{
						FlushBatch(requestStream, batch);
						return;
					}
					batch.Add(item);
					if (batch.Count >= options.BatchSize)
						break;
				}
				FlushBatch(requestStream, batch);
			}
		}

		public event Action<string> Report;

		public void Write(string id, RavenJObject metadata, RavenJObject data)
		{
			if (id == null) throw new ArgumentNullException("id");
			if (metadata == null) throw new ArgumentNullException("metadata");
			if (data == null) throw new ArgumentNullException("data");

			if (nextTask.IsCanceled || nextTask.IsFaulted)
				nextTask.Wait(); // error early if we have  any error

			metadata["@id"] = id;
			data[Constants.Metadata] = metadata;
			items.Add(data);
		}

		public void Dispose()
		{
			items.Add(null);
			nextTask.ContinueWith(task =>
			{
				task.AssertNotFailed();
				Action<string> report = Report;
				if (report != null)
				{
					report("Finished writing all results to server... waiting for the operation to finish...");
				}
				long id;

				using (var response = RawExecuteRequest(httpRavenRequest))
				using (var stream = response.GetResponseStream())
				using (var streamReader = new StreamReader(stream))
				{
					var result = RavenJObject.Load(new JsonTextReader(streamReader));
					id = result.Value<long>("OperationId");
				}

				while (true)
				{
					try
					{
						var request = CreateBulkRequest("/operation/status?id=" + id, "GET");
						var status = request.ExecuteRequest<RavenJToken>();
						if (status == null)
							break;
						if (status.Value<bool>("Completed"))
							break;
						Thread.Sleep(2000);
					}
					catch (WebException)
					{
						break;
					}
				}

				if (report != null)
				{
					report("Done.");
				}
			}).Wait();
		}

		private void FlushBatch(Stream requestStream, List<RavenJObject> localBatch)
		{
			if (localBatch.Count == 0)
				return;
			bufferedStream.SetLength(0);
			WriteToBuffer(localBatch);

			var requestBinaryWriter = new BinaryWriter(requestStream);
			requestBinaryWriter.Write((int)bufferedStream.Position);
			bufferedStream.WriteTo(requestStream);
			requestStream.Flush();

			total += localBatch.Count;
			Action<string> report = Report;
			if (report != null)
			{
				report(string.Format("Wrote {0:#,#} (total {2:#,#}) documents to server gzipped to {1:#,#.##} kb",
									 localBatch.Count,
									 bufferedStream.Position / 1024,
									 total));
			}
		}

		private void WriteToBuffer(List<RavenJObject> localBatch)
		{
			using (var gzip = new GZipStream(bufferedStream, CompressionMode.Compress, leaveOpen: true))
			{
				var binaryWriter = new BinaryWriter(gzip);
				binaryWriter.Write(localBatch.Count);
				var bsonWriter = new BsonWriter(binaryWriter);
				foreach (RavenJObject doc in localBatch)
				{
					doc.WriteTo(bsonWriter);
				}
				bsonWriter.Flush();
				binaryWriter.Flush();
				gzip.Flush();
			}
		}

		private HttpRavenRequest CreateBulkRequest(string url, string method)
		{
			var request = createRequest(url, method);

			PrepareForLongRequest(request);
			DisableCompression(request);

			return request;
		}

		private IDisposable Expect100Continue()
		{
			var servicePoint = ServicePointManager.FindServicePoint(new Uri(connectionOptions.Url));
			servicePoint.Expect100Continue = true;
			return new DisposableAction(() => servicePoint.Expect100Continue = false);
		}

		private static void PrepareForLongRequest(HttpRavenRequest ravenRequest)
		{
			ravenRequest.WebRequest.Timeout = (int)TimeSpan.FromHours(6).TotalMilliseconds;
			ravenRequest.WebRequest.AllowWriteStreamBuffering = false;
			ravenRequest.WebRequest.ContentLength = 0;
		}

		private static void DisableCompression(HttpRavenRequest ravenRequest)
		{
			ravenRequest.WebRequest.Headers.Remove("Content-Encoding");
			ravenRequest.WebRequest.Headers.Remove("Accept-Encoding");
		}

		private static Task<Stream> GetRawRequestStream(HttpRavenRequest ravenRequest)
		{
			ravenRequest.WebRequest.SendChunked = true;
			return Task.Factory.FromAsync<Stream>(ravenRequest.WebRequest.BeginGetRequestStream, ravenRequest.WebRequest.EndGetRequestStream, null);
		}

		private static WebResponse RawExecuteRequest(HttpRavenRequest ravenRequest)
		{
			try
			{
				return ravenRequest.WebRequest.GetResponse();
			}
			catch (WebException we)
			{
				var httpWebResponse = we.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				var sb = new StringBuilder()
					.Append(httpWebResponse.StatusCode)
					.Append(" ")
					.Append(httpWebResponse.StatusDescription)
					.AppendLine();

				using (var reader = new StreamReader(httpWebResponse.GetResponseStream()))
				{
					string line;
					while ((line = reader.ReadLine()) != null)
					{
						sb.AppendLine(line);
					}
				}
				throw new InvalidOperationException(sb.ToString(), we);
			}
		}
	}
}