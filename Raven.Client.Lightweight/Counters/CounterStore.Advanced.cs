using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Counters.Operations;
using Raven.Database.Counters;

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

				//TODO : perhaps this call should not be with failover? discuss with Oren
				var states = await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get,async (url, counterStoreName) =>
				{
					var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}/cs/{1}/sinceEtag/{2}?skip={3}&take={4}", url, counterStoreName, etag, skip, take);

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
