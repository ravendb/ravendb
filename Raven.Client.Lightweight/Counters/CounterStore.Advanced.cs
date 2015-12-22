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
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public async Task<IReadOnlyList<CounterSummary>> GetCounters(int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
            {
                return await parent.Admin.GetCountersByStorage(null,token,skip,take).ConfigureAwait(false);
            }

            public async Task<IReadOnlyList<CounterSummary>> GetCountersByPrefix(string groupName, string counterNamePrefix = null, int skip = 0, int take = 1024, CancellationToken token = default(CancellationToken))
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

	        
        }
    }
}
