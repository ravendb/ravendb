using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Client.Extensions;

namespace Raven.Client.Document
{
	public class RemoteBulkInsertOperation : IDisposable
	{
		private readonly HttpJsonRequest httpJsonRequest;
		private readonly Task nextTask;
		private readonly BlockingCollection<RavenJObject> items;
		private readonly MemoryStream bufferedStream = new MemoryStream();

		public event Action<string> Report;

		public RemoteBulkInsertOperation(BulkInsertOptions options, ServerClient client, int batchSize = 512)
		{
			items = new BlockingCollection<RavenJObject>(batchSize * 8);
			var requestUrl = "/bulkInsert?";
			if (options.CheckForUpdates)
				requestUrl += "checkForUpdates=true";
			if (options.CheckReferencesInIndexes)
				requestUrl += "&checkReferencesInIndexes=true";

			httpJsonRequest = client.CreateRequest("POST", requestUrl, disableRequestCompression: true);
			nextTask = httpJsonRequest.GetRawRequestStream()
				.ContinueWith(task =>
				{
					var requestStream = task.Result;
					while (true)
					{
						var batch = new List<RavenJObject>();
						RavenJObject item;
						while (items.TryTake(out item, 200))
						{
							if (item == null)// marker
							{
								FlushBatch(requestStream, batch);
								return;
							}
							batch.Add(item);
							if (batch.Count >= batchSize)
								break;
						}
						FlushBatch(requestStream, batch);
					}
				});
		}

		public void Write(string id, RavenJObject metadata, RavenJObject data)
		{
			metadata["@id"] = id;
			data[Constants.Metadata] = metadata;
			items.Add(data);
		}

		private int total = 0;
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
			var report = Report;
			if(report!=null)
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
				foreach (var doc in localBatch)
				{
					doc.WriteTo(bsonWriter);
				}
				bsonWriter.Flush();
				binaryWriter.Flush();
				gzip.Flush();
			}
		}

		public void Dispose()
		{
			items.Add(null);
			nextTask.ContinueWith(task =>
			{
				task.AssertNotFailed();
				var report = Report;
				if (report != null)
				{
					report("Finished writing all results to server");
				}
				httpJsonRequest.RawExecuteRequest();
				if (report != null)
				{
					report("Done writing to server");
				}
			}).Wait();
		}
	}
}