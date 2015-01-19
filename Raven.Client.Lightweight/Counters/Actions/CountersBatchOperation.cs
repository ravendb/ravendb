using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
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
		private bool disposed;
		private readonly CountersBatchOptions _options;
		private readonly MemoryStream _tempStream;
		private long serverOperationId;

		internal CountersBatchOperation(ICounterStore parent,string counterName, CountersBatchOptions options = null) : base(parent, counterName)
		{

			if(options != null && options.BatchSizeLimit < 1)
				throw new ArgumentException("options");

			_options = options ?? new CountersBatchOptions(); //defaults do exist
			_changesQueue = new BlockingCollection<CounterChange>();			
			_cts = new CancellationTokenSource();
			_batchOperationTcs = new TaskCompletionSource<bool>();
			_tempStream = new MemoryStream();
			_batchOperationTask = StartBatchOperation();
			disposed = false;
		}

		private async Task StartBatchOperation()
		{
			using (ConnectionOptions.Expect100Continue(serverUrl))
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

					long operationId;

					using (response)
					{
						using (var stream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
						{
							var result = RavenJToken.TryLoad(stream); 
							if (result == null) //precaution - should prevent NRE in case the crap hits the fan
								throw new ApplicationException("Invalid response from server...maybe its not json?");

							operationId = result.Value<long>("OperationId");
						}
					}
					
					if (await IsOperationCompleted(operationId).ConfigureAwait(false))
						serverOperationId = operationId;
					_batchOperationTcs.SetResult(true);
				}
			}
		}

		private async Task<bool> IsOperationCompleted(long operationId)
		{
			ErrorResponseException errorResponse;

			try
			{
				var status = await GetOperationStatusAsync(operationId);

				if (status == null) return true;

				if (status.Value<bool>("Completed"))
					return true;

				return false;
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode != HttpStatusCode.Conflict)
					throw;

				errorResponse = e;
			}

			var conflictsDocument = RavenJObject.Load(new RavenJsonTextReader(new StringReader(errorResponse.ResponseString)));

			throw new ConcurrencyException(conflictsDocument.Value<string>("Error"));
		}

		private async Task<RavenJToken> GetOperationStatusAsync(long id)
		{
			var url = serverUrl + "/operation/status?id=" + id;
			using (var request = CreateHttpJsonRequest(url, Verbs.Get))
			{
				try
				{
					return await request.ReadResponseJsonAsync().ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound) return null;
					throw;
				}
			}
		}

		private void ContinuouslyWriteQueueToServer(Stream stream, CancellationToken token)
		{
			try
			{
				while (_changesQueue.IsCompleted == false)
				{
					token.ThrowIfCancellationRequested();

					var batch = new List<CounterChange>(_options.BatchSizeLimit);
					CounterChange counterChange;
					while (_changesQueue.TryTake(out counterChange, _options.BatchReadTimeoutInMilliseconds))
					{
						batch.Add(counterChange);
						if (batch.Count == _options.BatchSizeLimit)
							break;
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
			WriteCollectionToBuffer(_tempStream, AggregateItems(batchItems), out bytesWritten);

			var requestBinaryWriter = new BinaryWriter(requestStream);
			requestBinaryWriter.Write((int)_tempStream.Position);

			_tempStream.WriteTo(requestStream);
			requestStream.Flush();
		}

		private ICollection<CounterChange> AggregateItems(IEnumerable<CounterChange> batchItems)
		{
			var aggregationResult = from item in batchItems
									group item by new { item.Name, item.Group }
										into g
										select new CounterChange
										{
											Name = g.Key.Name,
											Group = g.Key.Group,
											Delta = g.Sum(x => x.Delta)
										};

			return aggregationResult.ToList();
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

		public void Change(string group, long delta)
		{
			_changesQueue.Add(new CounterChange
			{
				Delta = delta,
				Group = group,
				Name = counterName
			});
		}

		public void Increment(string group)
		{
			Change(@group, 1);
		}

		public void Decrement(string group)
		{
			Change(@group, -1);
		}

		public void Dispose()
		{
			if (!disposed)
			{
				_changesQueue.CompleteAdding();

				_batchOperationTcs.Task.Wait();
				if (_batchOperationTask.Status != TaskStatus.RanToCompletion ||
					_batchOperationTask.Status != TaskStatus.Canceled)
					_cts.Cancel();

				if (serverOperationId != default(long))
				{
					while (true)
					{
						var serverSideOperationWaitingTask = IsOperationCompleted(serverOperationId);
						serverSideOperationWaitingTask.Wait();

						if (serverSideOperationWaitingTask.Result)
							break;

						Thread.Sleep(100);
					}
				}

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
