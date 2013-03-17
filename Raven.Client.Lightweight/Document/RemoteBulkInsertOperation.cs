#if !NETFX_CORE
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#else
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
#endif
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
#if !SILVERLIGHT
using System.IO.Compression;
using Raven.Client.Extensions;
#else
using Raven.Client.Connection.Async;
using Raven.Client.Silverlight.Connection;
using Ionic.Zlib;
#endif

namespace Raven.Client.Document
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
#if !SILVERLIGHT
		private readonly ServerClient client;
#else
		private readonly AsyncServerClient client;
#endif
		private readonly MemoryStream bufferedStream = new MemoryStream();
		private HttpJsonRequest httpJsonRequest;
		private readonly BlockingCollection<RavenJObject> items;

		private Task nextTask;
		private int total;

#if !SILVERLIGHT
		public RemoteBulkInsertOperation(BulkInsertOptions options, ServerClient client)
#else
		public RemoteBulkInsertOperation(BulkInsertOptions options, AsyncServerClient client)
#endif
		{
			this.options = options;
			this.client = client;
			items = new BlockingCollection<RavenJObject>(options.BatchSize * 8);
			string requestUrl = "/bulkInsert?";
			if (options.CheckForUpdates)
				requestUrl += "checkForUpdates=true";
			if (options.CheckReferencesInIndexes)
				requestUrl += "&checkReferencesInIndexes=true";

#if !SILVERLIGHT
			var expect100Continue = client.Expect100Continue();

			// this will force the HTTP layer to authenticate, meaning that our next request won't have to
			HttpJsonRequest req = client.CreateRequest("POST", requestUrl + "&op=generate-single-use-auth-token",
														disableRequestCompression: true);
			var token = req.ReadResponseJson();

			httpJsonRequest = client.CreateRequest("POST", requestUrl, disableRequestCompression: true);

			// the request may take a long time to process, so we need to set a large timeout value
			httpJsonRequest.PrepareForLongRequest();
			httpJsonRequest.AddOperationHeader("Single-Use-Auth-Token", token.Value<string>("Token"));
			nextTask = httpJsonRequest.GetRawRequestStream()
									  .ContinueWith(task =>
									  {
										  try
										  {
											  expect100Continue.Dispose();
										  }
										  catch (Exception)
										  {
										  }
										  WriteQueueToServer(task);
									  });
#else
			var request = client
				.CreateRequest(requestUrl + "&no-op=for-auth-only", "POST", disableRequestCompression: true);
			request.PrepareForLongRequest();

			request
				.ExecuteRequestAsync()
				.ContinueWith(task =>
				{
					httpJsonRequest = client.CreateRequest(requestUrl, "POST", disableRequestCompression: true);
					httpJsonRequest.PrepareForLongRequest();
					httpJsonRequest.webRequest.ContentLength = 0;

					nextTask = httpJsonRequest
						.GetRawRequestStream()
						.ContinueWith(WriteQueueToServer);
				})
				.Wait();
#endif
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
					report("Finished writing all results to server");
				}

				long id = -1;

#if !SILVERLIGHT
				using (var response = httpJsonRequest.RawExecuteRequest())
				using (var stream = response.GetResponseStream())
				using (var streamReader = new StreamReader(stream))
				{
					var result = RavenJObject.Load(new JsonTextReader(streamReader));
					id = result.Value<long>("OperationId");
				}
#else
				httpJsonRequest.RawExecuteRequestAsync()
				               .ContinueWith(t =>
				               {
					               var response = t.Result;
					               using (var stream = response.GetResponseStream())
					               {
						               using (var streamReader = new StreamReader(stream))
						               {
							               var result = RavenJObject.Load(new JsonTextReader(streamReader));
							               id = result.Value<long>("OperationId");
						               }
					               }
				               })
				               .Wait();
#endif
				while (true)
				{
#if !SILVERLIGHT
					var status = client.GetOperationStatus(id);
#else
					var status = client.GetOperationStatusAsync(id).Result;
#endif
					if (status == null)
						break;
					if (status.Value<bool>("Completed"))
						break;
					Thread.Sleep(500);
				}

				if (report != null)
				{
					report("Done writing to server");
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
				report(string.Format("Wrote {0:#,#} (total {2:#,#} documents to server gzipped to {1:#,#.##} kb",
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
	}
}
#endif
