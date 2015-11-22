using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Implementation;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
	public partial class CounterStore
	{
		public class CounterStreams
		{
			private readonly CounterStore parent;

			public CounterStreams(CounterStore parent)
			{
				this.parent = parent;
			}

			public async Task<IAsyncEnumerator<CounterGroup>> CounterGroups(int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();
				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().WithCancellation(token).ConfigureAwait(false);
				try
				{
					return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, counterStoreName) =>
					{
						var requestUriString = $"{url}/cs/{counterStoreName}/streams/groups?skip={skip}&take={take}&format=json";
						var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get);

						var response = await request.ExecuteRawResponseAsync().WithCancellation(token).ConfigureAwait(false);
						response.EnsureSuccessStatusCode();

	                    var stream = await response.GetResponseStreamWithHttpDecompression().WithCancellation(token).ConfigureAwait(false);

						return new CounterGroupEnumerator(request, stream);
					}, token).ConfigureAwait(false);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Failed to stream counter groups. See inner exception for details", e);
				}
			}

			public async Task<IAsyncEnumerator<CounterSummary>> CounterSummariesByPrefix(string @group, string counterNamePrefix, int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
			{
				if (string.IsNullOrWhiteSpace(@group))
					throw new ArgumentNullException(nameof(@group));

				parent.AssertInitialized();
				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().WithCancellation(token).ConfigureAwait(false);

				try
				{
					return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, counterStoreName) =>
					{
						var requestUriString = $"{url}/cs/{counterStoreName}/streams/summaries?group={@group}&skip={skip}" +
						                       $"&take={take}&format=json&counterNamePrefix={counterNamePrefix}";
						var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get);

						var response = await request.ExecuteRawResponseAsync().WithCancellation(token).ConfigureAwait(false);
						response.EnsureSuccessStatusCode();
						var stream = await response.GetResponseStreamWithHttpDecompression().WithCancellation(token).ConfigureAwait(false);

						return new CounterSummaryEnumerator(request, stream);
					}, token).ConfigureAwait(false);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Failed to stream counter summaries. See inner exception for details", e);
				}
			}

			public async Task<IAsyncEnumerator<CounterSummary>> CounterSummaries(string @group = null, int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();
				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().WithCancellation(token).ConfigureAwait(false);

				try
				{
					return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, counterStoreName) =>
					{
						var requestUriString = $"{url}/cs/{counterStoreName}/streams/summaries?group={@group}&skip={skip}&take={take}&format=json";
						var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get);

						var response = await request.ExecuteRawResponseAsync().WithCancellation(token).ConfigureAwait(false);
						response.EnsureSuccessStatusCode();
						var stream = await response.GetResponseStreamWithHttpDecompression().WithCancellation(token).ConfigureAwait(false);

						return new CounterSummaryEnumerator(request, stream);
					}, token).ConfigureAwait(false);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Failed to stream counter summaries. See inner exception for details",e);
				}
			}

			public class CounterGroupEnumerator : StreamEnumerator<CounterGroup>
			{
				public CounterGroupEnumerator(HttpJsonRequest request, Stream stream) : base(request, stream)
				{
				}

				public override void SetCurrent()
				{
					Current = enumerator.Current.ToObject<CounterGroup>();
				}
			}

			public class CounterSummaryEnumerator : StreamEnumerator<CounterSummary>
			{
				public CounterSummaryEnumerator(HttpJsonRequest request, Stream stream) : base(request, stream)
				{
				}

				public override void SetCurrent()
				{
					Current = enumerator.Current.ToObject<CounterSummary>();
				}
			}

			public abstract class StreamEnumerator<T> : IAsyncEnumerator<T> where T : class
			{
				protected readonly YieldStreamResults enumerator;

				protected StreamEnumerator(HttpJsonRequest request, Stream stream)
				{
					this.enumerator = new YieldStreamResults(request, stream);
				}

				public void Dispose()
				{
					enumerator.Dispose();
				}

				public async Task<bool> MoveNextAsync()
				{
					var moveNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
					if (moveNext)
						SetCurrent();
					else
						Current = default(T);
					return moveNext;
				}

				public abstract void SetCurrent();

				public T Current { get; protected set; }
			}

			public class YieldStreamResults : IAsyncEnumerator<RavenJObject>
			{
				private readonly HttpJsonRequest request;
				private readonly Stream stream;
				private readonly JsonTextReaderAsync reader;
				private readonly StreamReader streamReader;
				private bool complete;
				private bool wasInitialized;

				public YieldStreamResults(HttpJsonRequest request, Stream stream)
				{
					this.request = request;
					this.stream = stream;
					streamReader = new StreamReader(stream);
					reader = new JsonTextReaderAsync(streamReader);
				}

				private async Task InitAsync()
				{
					if (await reader.ReadAsync().ConfigureAwait(false) == false ||
							reader.TokenType != JsonToken.StartObject)
						throw new InvalidOperationException("Unexpected data at start of stream. This exception should not happen and is probably a bug.");

					if (await reader.ReadAsync().ConfigureAwait(false) == false ||
							reader.TokenType != JsonToken.PropertyName ||
							Equals("Results", reader.Value) == false)
						throw new InvalidOperationException("Unexpected data at stream 'Results' property name. This exception should not happen and is probably a bug.");

					var readResult = await reader.ReadAsync().ConfigureAwait(false);
					if (readResult == false ||
							reader.TokenType != JsonToken.StartArray)
						throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array. This exception should not happen and is probably a bug.");
				}

				public void Dispose()
				{
					try
					{
						reader.Close();
					}
					catch (Exception)
					{
					}
					try
					{
						streamReader.Close();
					}
					catch (Exception)
					{

					}
					try
					{
						stream.Close();
					}
					catch (Exception)
					{

					}
					try
					{
						request.Dispose();
					}
					catch (Exception)
					{

					}
				}

				public async Task<bool> MoveNextAsync()
				{
					if (complete)
					{
						// to parallel IEnumerable<T>, subsequent calls to MoveNextAsync after it has returned false should
						// also return false, rather than throwing
						return false;
					}

					if (wasInitialized == false)
					{
						await InitAsync().ConfigureAwait(false);
						wasInitialized = true;
					}

					if (await reader.ReadAsync().ConfigureAwait(false) == false)
						throw new InvalidOperationException("Unexpected end of data. This exception should not happen and is probably a bug.");

					if (reader.TokenType == JsonToken.EndArray)
					{
						complete = true;

						await EnsureValidEndOfResponse().ConfigureAwait(false);
						Dispose();
						return false;
					}

					Current = (RavenJObject)await RavenJToken.ReadFromAsync(reader).ConfigureAwait(false);
					return true;
				}

				private async Task EnsureValidEndOfResponse()
				{
					if (reader.TokenType != JsonToken.EndObject && await reader.ReadAsync().ConfigureAwait(false) == false)
						throw new InvalidOperationException("Unexpected end of response - missing EndObject token. This exception should not happen and is probably a bug.");

					if (reader.TokenType != JsonToken.EndObject)
						throw new InvalidOperationException($"Unexpected token type at the end of the response: {reader.TokenType}. Error: {streamReader.ReadToEnd()}. This exception should not happen and is probably a bug.");

					var remainingContent = await streamReader.ReadToEndAsync().ConfigureAwait(false);

					if (string.IsNullOrEmpty(remainingContent) == false)
						throw new InvalidOperationException("Server error: " + remainingContent);
				}

				public RavenJObject Current { get; private set; }
			}
		}
	}
}
