using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Client.Extensions;

namespace Raven.Client.Document
{
	public class RemoteBulkInsertOperation : IDisposable
	{
		private readonly int batchSize;
		private readonly HttpJsonRequest httpJsonRequest;
		private Task nextTask;
		private readonly BlockingCollection<RavenJObject> items;

		public RemoteBulkInsertOperation(ServerClient client, int batchSize = 2048)
		{
			items = new BlockingCollection<RavenJObject>();
			this.batchSize = batchSize;
			httpJsonRequest = client.CreateRequest("POST", "/bulkInsert", disableRequestCompression: true);
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

		private void FlushBatch(Stream requestStream, List<RavenJObject> localBatch)
		{
			if (localBatch.Count == 0)
				return;

			var binaryWriter = new BinaryWriter(requestStream);
			binaryWriter.Write(localBatch.Count);
			var bsonWriter = new BsonWriter(binaryWriter);
			foreach (var doc in localBatch)
			{
				doc.WriteTo(bsonWriter);
			}
			bsonWriter.Flush();
			binaryWriter.Flush();
		}

		public void Dispose()
		{
			items.Add(null);
			nextTask.ContinueWith(task =>
			{
				task.AssertNotFailed();
				httpJsonRequest.RawExecuteRequest();
			}).Wait();
		}
	}
}