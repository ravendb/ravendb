using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Counters
{
    public partial class CounterStore
    {
        public class CounterStoreAdminOperations
        {
            private readonly CounterStore parent;
            
            internal CounterStoreAdminOperations(CounterStore parent)
            {
                this.parent = parent;
            }

            public async Task<IReadOnlyList<CounterNameGroupPair>> GetAllCounterStorageNameAndGroups(string counterStorageName = null,
                CancellationToken token = default(CancellationToken))
            {
                var nameGroupPairs = new List<CounterNameGroupPair>();
                var taken = 0;
                IReadOnlyList<CounterNameGroupPair> nameGroupPairsTaken;
                do
                {
                    nameGroupPairsTaken = await GetCounterStorageNameAndGroups(counterStorageName, token, taken).ConfigureAwait(false);
                    taken += nameGroupPairsTaken.Count;
                    if (nameGroupPairsTaken.Count > 0)
                        nameGroupPairs.AddRange(nameGroupPairsTaken);
                } while (nameGroupPairsTaken.Count > 0);

                return nameGroupPairs.ToList();
            }

            public async Task<IReadOnlyList<CounterNameGroupPair>> GetCounterStorageNameAndGroups(string counterStorageName = null, 
                CancellationToken token = default(CancellationToken),
                int skip = 0, int take = 1024)
            {
                parent.AssertInitialized();

                var requestUriString = $"{parent.Url}/admin/cs/{counterStorageName ?? parent.Name}?op=groups-names&skip={skip}&take={take}";

                using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
                {
                    var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return response.ToObject<List<CounterNameGroupPair>>(parent.JsonSerializer);
                }
            }

            public async Task<IReadOnlyList<CounterSummary>> GetAllCounterStorageSummaries(string counterStorageName = null,
                CancellationToken token = default(CancellationToken))
            {
                var summaries = new List<CounterSummary>();
                var taken = 0;
                IReadOnlyList<CounterSummary> summariesTaken;
                do
                {
                    summariesTaken = await GetCountersByStorage(counterStorageName, token, taken).ConfigureAwait(false);
                    taken += summariesTaken.Count;
                    if(summariesTaken.Count > 0)
                        summaries.AddRange(summariesTaken);
                } while (summariesTaken.Count > 0);

                return summaries.ToArray();
            }

            public async Task<IReadOnlyList<CounterSummary>> GetCountersByStorage(string counterStorageName, 
                CancellationToken token = default(CancellationToken),
                int skip = 0,int take = 1024)
            {
                parent.AssertInitialized();

                var requestUriString = $"{parent.Url}/admin/cs/{counterStorageName ?? parent.Name}?op=summary&skip={skip}&take={take}";

                using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
                {
                    var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return response.ToObject<List<CounterSummary>>(parent.JsonSerializer);
                }
            }

            /// <summary>
            /// Create new counter storage on the server.
            /// </summary>
            /// <param name="counterStorageDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
            /// <param name="counterStorageName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
            /// <param name="shouldUpateIfExists">If the storage already there, should we update it</param>
            /// <param name="credentials">Credentials used for this operation.</param>
            /// <param name="token">Cancellation token used for this operation.</param>
            public async Task<CounterStore> CreateCounterStorageAsync(CounterStorageDocument counterStorageDocument, 
                string counterStorageName, 
                bool shouldUpateIfExists = false,
                OperationCredentials credentials = null, 
                CancellationToken token = default(CancellationToken))
            {
                if (counterStorageDocument == null)
                    throw new ArgumentNullException(nameof(counterStorageDocument));

                if (counterStorageName == null) throw new ArgumentNullException(nameof(counterStorageName));

                parent.AssertInitialized();

                var urlTemplate = "{0}/admin/cs/{1}";
                if (shouldUpateIfExists)
                    urlTemplate += "?update=true";

                var requestUriString = String.Format(urlTemplate, parent.Url, counterStorageName);

                using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
                {
                    try
                    {
                        await request.WriteAsync(RavenJObject.FromObject(counterStorageDocument)).WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.Conflict)
                            throw new InvalidOperationException($"Cannot create counter storage with the name '{counterStorageName}' because it already exists. Use shouldUpateIfExists = true flag in case you want to update an existing counter storage", e);

                        throw;
                    }
                }

                return new CounterStore
                {
                    Name = counterStorageName,
                    Url = parent.Url,
                    Credentials = credentials ?? parent.Credentials
                };
            }

            public async Task DeleteCounterStorageAsync(string counterStorageName, bool hardDelete = false, CancellationToken token = default(CancellationToken))
            {
                parent.AssertInitialized();

                var requestUriString = $"{parent.Url}/admin/cs/{counterStorageName}?hard-delete={hardDelete}";

                using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                {
                    try
                    {
                        await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound)
                            throw new InvalidOperationException($"Counter storage with specified name ({counterStorageName}) doesn't exist");
                        throw;
                    }
                }
            }

            public async Task<IReadOnlyList<string>> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken))
            {
                parent.AssertInitialized();

                var requestUriString = $"{parent.Url}/cs";

                using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
                {
                    var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return response.ToObject<IReadOnlyList<string>>(parent.JsonSerializer);
                }
            }
             
        }
    }
}
