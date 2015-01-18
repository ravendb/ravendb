using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Client.Extensions;

namespace Raven.Client.Counters.Actions
{
	public class CountersBatchOperation : CountersActionsBase, IDisposable
	{
		private readonly Task _batchOperationTask;
		private readonly TaskCompletionSource<bool> _batchOperationTcs; 
		private readonly BlockingCollection<CounterChange> _changesQueue;
		private readonly CancellationTokenSource _cts;
		private bool _isOperationRunning;
		private bool disposed;
		private readonly CountersBatchOptions _options;
		private readonly MemoryStream _tempStream;

		internal CountersBatchOperation(CountersClient parent, CountersBatchOptions options = null) : base(parent)
		{
			_options = options ?? new CountersBatchOptions(); //defaults do exist
			_changesQueue = new BlockingCollection<CounterChange>();			
			_cts = new CancellationTokenSource();
			_batchOperationTcs = new TaskCompletionSource<bool>();
			_tempStream = new MemoryStream();
			_isOperationRunning = true;
			_batchOperationTask = StartBatchOperation();
			disposed = false;
		}

		private async Task StartBatchOperation()
		{
			//using (ConnectionOptions.Expect100Continue(parent.ServerUrl))
			{
				var requestUriString = String.Format("{0}/batch", counterStorageUrl);
				using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
				{
					var token = _cts.Token;
					var response = await request.ExecuteRawRequestAsync((stream, source) => Task.Factory.StartNew(() =>
					{
						try
						{
							ContinuouslyWriteQueueToServer(stream, token);
							source.TrySetResult(null);
						}
						catch (Exception e)
						{
							source.TrySetException(e);
							_batchOperationTcs.SetException(e);
						}
					}, TaskCreationOptions.LongRunning)).ConfigureAwait(false);

					await response.AssertNotFailingResponse();

					/*long operationId;

					using (response)
					using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
					using (var streamReader = new StreamReader(stream))
					{
						var result = RavenJObject.Load(new JsonTextReader(streamReader));
						operationId = result.Value<long>("OperationId");
					}*/

					//TODO: implement changes api and server side operation tracking
					//if (await IsOperationCompleted(operationId).ConfigureAwait(false)) responseOperationId = operationId;

					_batchOperationTcs.SetResult(true);
				}
			}
		}

		private void ContinuouslyWriteQueueToServer(Stream stream, CancellationToken token)
		{
			try
			{
				while (_isOperationRunning)
				{
					token.ThrowIfCancellationRequested();

					var batch = new List<CounterChange>(_options.BatchSizeLimit);
					CounterChange counterChange;
					while (_changesQueue.TryTake(out counterChange, _options.BatchReadTimeoutInMilliseconds))
					{
						if (batch.Count >= _options.BatchSizeLimit)
							break;
						batch.Add(counterChange);
					}

					FlushToServer(stream, batch);
				}
			}
			finally
			{
				_tempStream.Dispose();
			}
		}

		private void FlushToServer(Stream requestStream, ICollection<CounterChange> batchItems)
		{
			if (batchItems.Count == 0)
				return;

			_tempStream.SetLength(0);
			long bytesWritten;
			WriteCollectionToBuffer(_tempStream, batchItems, out bytesWritten);

			var requestBinaryWriter = new BinaryWriter(requestStream);
			requestBinaryWriter.Write((int)_tempStream.Position);

			_tempStream.WriteTo(requestStream);
			requestStream.Flush();
		}

		private void WriteCollectionToBuffer(Stream targetStream, ICollection<CounterChange> items, out long bytesWritten)
		{
			using (var gzip = new GZipStream(targetStream, CompressionMode.Compress, leaveOpen: true))
			using (var stream = new CountingStream(gzip))
			{
				var binaryWriter = new BinaryWriter(stream);
				binaryWriter.Write(items.Count);
				var bsonWriter = new BsonWriter(binaryWriter)
				{
					DateTimeKindHandling = DateTimeKind.Unspecified
				};

				foreach (var doc in items.Select(RavenJObject.FromObject))
					doc.WriteTo(bsonWriter);

				bsonWriter.Flush();
				binaryWriter.Flush();
				stream.Flush();
				bytesWritten = stream.NumberOfWrittenBytes;
			}
		}

		public void Change(string group, string counterName, long delta)
		{
			_changesQueue.Add(new CounterChange
			{
				Delta = delta,
				Group = group,
				Name = counterName
			});
		}

		public void Increment(string group, string counterName)
		{
			Change(@group, counterName, 1);
		}

		public void Decrement(string group, string counterName)
		{
			Change(@group, counterName, -1);
		}

		public void Dispose()
		{
			if (!disposed)
			{
				_changesQueue.CompleteAdding();
				_isOperationRunning = false;

				_batchOperationTcs.Task.Wait();
				if (_batchOperationTask.Status != TaskStatus.RanToCompletion ||
					_batchOperationTask.Status != TaskStatus.Canceled)
					_cts.Cancel();

				_tempStream.Dispose(); //precaution
				disposed = true;
			}

		}

		private async Task<string> GetToken()
		{
			// this will force the HTTP layer to authenticate, meaning that our next request won't have to
			var jsonToken = await GetAuthToken().ConfigureAwait(false);

			return jsonToken.Value<string>("Token");
		}

		private async Task<RavenJToken> GetAuthToken()
		{
			using (var request = CreateHttpJsonRequest("/singleAuthToken", Verbs.Get, disableRequestCompression: true))
			{
				return await request.ReadResponseJsonAsync().ConfigureAwait(false);
			}
		}

		private async Task<string> ValidateThatWeCanUseAuthenticateTokens(string token)
		{
			using (var request = CreateHttpJsonRequest("/singleAuthToken", Verbs.Get, disableRequestCompression: true, disableAuthentication: true))
			{
				request.AddOperationHeader("Single-Use-Auth-Token", token);
				var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
				return result.Value<string>("Token");
			}
		}
	}
}
