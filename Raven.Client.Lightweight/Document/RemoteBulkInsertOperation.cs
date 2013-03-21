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
using Raven.Client.Connection.Async;
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
		private readonly AsyncServerClient asyncClient;
		private readonly MemoryStream bufferedStream = new MemoryStream();
		private HttpJsonRequest httpJsonRequest;
		private readonly BlockingCollection<RavenJObject> items;

		private bool disposed;
		private readonly Task nextTask;
		private int total;
		private readonly string requestUrl;

		public RemoteBulkInsertOperation(BulkInsertOptions options, AsyncServerClient asyncClient)
		{
			this.options = options;
			this.asyncClient = asyncClient;
			items = new BlockingCollection<RavenJObject>(options.BatchSize*8);
			requestUrl = "/bulkInsert?";
			if (options.CheckForUpdates)
				requestUrl += "checkForUpdates=true";
			if (options.CheckReferencesInIndexes)
				requestUrl += "&checkReferencesInIndexes=true";

			nextTask = StartAsync();
		}

		private async Task StartAsync()
		{
			using (ConnectionOptions.Expect100Continue(asyncClient.Url))
			{
				// this will force the HTTP layer to authenticate, meaning that our next request won't have to
				HttpJsonRequest req = asyncClient.CreateRequest("POST", requestUrl + "&op=generate-single-use-auth-token",
				                                                disableRequestCompression: true);
				var token = await req.ReadResponseJsonAsync();

				httpJsonRequest = asyncClient.CreateRequest("POST", requestUrl, disableRequestCompression: true);

				// the request may take a long time to process, so we need to set a large timeout value
				httpJsonRequest.PrepareForLongRequest();
				httpJsonRequest.AddOperationHeader("Single-Use-Auth-Token", token.Value<string>("Token"));
				WriteQueueToServer(httpJsonRequest.GetRawRequestStream());
			}
		}

		private async void WriteQueueToServer(Task<Stream> task)
		{
			Stream requestStream = await task;
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

		public async Task DisposeAsync()
		{
			items.Add(null);
			await nextTask;
			var report = Report;
			if (report != null)
			{
				report("Finished writing all results to server");
			}

			long id;

			var response = await httpJsonRequest.RawExecuteRequestAsync();
			using (var stream = response.GetResponseStream())
			{
				using (var streamReader = new StreamReader(stream))
				{
					var result = await RavenJObject.LoadAsync(new JsonTextReaderAsync(streamReader));
					id = result.Value<long>("OperationId");
				}
			}

			while (true)
			{
				var status = await asyncClient.GetOperationStatusAsync(id);
				if (status == null)
					break;
				if (status.Value<bool>("Completed"))
					break;
				await TaskEx.Delay(500);
			}

			if (report != null)
			{
				report("Done writing to server");
			}

			disposed = true;
		}

		public void Dispose()
		{
			if (disposed)
				return;
#if !SILVERLIGHT
			DisposeAsync().Wait();
#else
			throw new InvalidOperationException("Dispose can only be called after DisposeAsync has completed");
#endif

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
