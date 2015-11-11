using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Counters.Operations;
using Raven.Database.Counters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
    public partial class CounterStore
    {
        public class CounterStoreAdvancedOperations
        {
            private readonly CounterStore parent;

            internal CounterStoreAdvancedOperations(CounterStore parent)
            {
                this.parent = parent;
				Stream = new CounterStreams(this);
            }

	        public CounterStreams Stream { get; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public async Task<IReadOnlyList<CounterSummary>> GetCounters(int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
            {
                return await parent.Admin.GetCountersByStorage(null,token,skip,take).ConfigureAwait(false);
            }

            public async Task<IReadOnlyList<CounterSummary>> GetCountersByPrefix(string groupName, int skip = 0, int take = 1024, string counterNamePrefix = null, CancellationToken token = default(CancellationToken))
            {
                if(string.IsNullOrWhiteSpace(groupName))
                    throw new ArgumentNullException(nameof(groupName));

                parent.AssertInitialized();
                await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);

                var summaries = await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, counterStoreName) =>
                {
                    var requestUriString = $"{url}/cs/{counterStoreName}/by-prefix?skip={skip}&take={take}&groupName={groupName}";
                    if (!string.IsNullOrWhiteSpace(counterNamePrefix))
                        requestUriString += $"&counterNamePrefix={counterNamePrefix}";

                    using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
                    {
                        var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        return response.ToObject<List<CounterSummary>>();
                    }
                }, token).ConfigureAwait(false);

                return summaries;
            }

            public CountersBatchOperation NewBatch(CountersBatchOptions options = null)
            {
                if (parent.Name == null)
                    throw new ArgumentException("Counter Storage isn't set!");

                parent.AssertInitialized();

                return new CountersBatchOperation(parent, parent.Name, options);
            }

            public async Task<List<CounterState>> GetCounterStatesSinceEtag(long etag, int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
            {
                parent.AssertInitialized();
                await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);

                var states = await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get,async (url, counterStoreName) =>
                {
                    var requestUriString = $"{url}/cs/{counterStoreName}/sinceEtag?etag={etag}&skip={skip}&take={take}";

                    using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
                    {
                        var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        return response.ToObject<List<CounterState>>();
                    }
                }, token).ConfigureAwait(false);
                
                return states;
            }

	        public class CounterStreams
	        {
		        private readonly CounterStoreAdvancedOperations advancedOperations;

		        public CounterStreams(CounterStoreAdvancedOperations advancedOperations)
		        {
			        this.advancedOperations = advancedOperations;
		        }

				public async Task<IAsyncEnumerator<CounterSummary>> CounterSummaries(string @group, int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
				{
					advancedOperations.parent.AssertInitialized();
					await advancedOperations.parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);

					return await advancedOperations.parent.ReplicationInformer.ExecuteWithReplicationAsync(advancedOperations.parent.Url, HttpMethods.Get, async (url, counterStoreName) =>
					{
						var requestUriString = $"{url}/cs/{counterStoreName}/streams/summaries?group={@group}&start={skip}&pageSize={take}&format=json";
						var request = advancedOperations.parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get);

						var response = await request.ExecuteRawResponseAsync().WithCancellation(token).ConfigureAwait(false);
						var stream = await response.GetResponseStreamWithHttpDecompression().WithCancellation(token).ConfigureAwait(false);

						return new CounterSummaryEnumerator(request, stream);
					}, token).ConfigureAwait(false);
				}

				public class CounterSummaryEnumerator : StreamEnumerator<CounterSummary>
		        {
			        public CounterSummaryEnumerator(HttpJsonRequest request, Stream stream) : base(request,stream)
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
				        this.enumerator = new YieldStreamResults(request,stream);
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
							throw new InvalidOperationException("Unexpected data at start of stream");

						if (await reader.ReadAsync().ConfigureAwait(false) == false || 
								reader.TokenType != JsonToken.PropertyName || 
								Equals("Results", reader.Value) == false)
							throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

						var readResult = await reader.ReadAsync().ConfigureAwait(false);
						if (readResult == false || 
								reader.TokenType != JsonToken.StartArray)
							throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");
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
							throw new InvalidOperationException("Unexpected end of data");

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
							throw new InvalidOperationException("Unexpected end of response - missing EndObject token");

						if (reader.TokenType != JsonToken.EndObject)
							throw new InvalidOperationException($"Unexpected token type at the end of the response: {reader.TokenType}. Error: {streamReader.ReadToEnd()}");

						var remainingContent = await streamReader.ReadToEndAsync().ConfigureAwait(false);

						if (string.IsNullOrEmpty(remainingContent) == false)
							throw new InvalidOperationException("Server error: " + remainingContent);
					}

					public RavenJObject Current { get; private set; }
				}
	        }
        }
    }
}
