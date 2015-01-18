using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Counters.Actions
{
	public class CountersBatchCommands : CountersActionsBase
	{
		internal CountersBatchCommands(CountersClient parent)
			: base(parent)
		{
		}

		public async Task ChangeAsync(string group, string counterName, long delta, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format(CultureInfo.InvariantCulture,"{0}/change?group={1}&counterName={2}&delta={3}",
				counterStorageUrl, @group, counterName, delta);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
		}

		public async Task IncrementAsync(string group, string counterName, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(@group, counterName, 1, token);
		}

		public async Task DecrementAsync(string group, string counterName, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(@group, counterName, -1, token);
		}

		public async Task ResetAsync(string group, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/change?group={1}&counterName={2}", counterStorageUrl, @group, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
		}

		public async Task<long> GetOverallTotalAsync(string group, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterOverallTotal?group={1}&counterName={2}", counterStorageUrl, @group, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false); ;
				return response.Value<long>();
			}
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string group, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterServersValues?group={1}&counterName={2}", counterStorageUrl, @group, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false); ;
				return response.ToObject<List<CounterView.ServerValue>>();
			}
		}

		#region CounterBatch stuff - To Be Deleted - not used currently

		//		public CounterBatch CreateBatch()
//		{
//			return new CounterBatch(parent, convention);
//		}
//
//		public class CounterBatch : IHoldProfilingInformation
//		{
//			private readonly OperationCredentials credentials;
//			private readonly HttpJsonRequestFactory jsonRequestFactory;
//			private readonly string counterStorageUrl;
//			private readonly Convention convention;
//
//			private readonly ConcurrentDictionary<string, long> counterData = new ConcurrentDictionary<string, long>();
//
//			public CounterBatch(CountersClient countersClient, Convention convention)
//			{
//				credentials = countersClient.PrimaryCredentials;
//				jsonRequestFactory = countersClient.JsonRequestFactory;
//				counterStorageUrl = countersClient.CounterStorageUrl;
//				this.convention = convention;
//			}
//
//			public ProfilingInformation ProfilingInformation { get; private set; }
//
//			public void Change(string group, string counterName, long delta)
//			{
//				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] {@group, counterName});
//
//				counterData.AddOrUpdate(counterFullName, delta, (key, existingVal) => existingVal + delta);
//			}
//
//			public void Increment(string group, string counterName)
//			{
//				Change(@group, counterName, 1);
//			}
//
//			public void Decrement(string group, string counterName)
//			{
//				Change(@group, counterName, -1);
//			}
//
//			public async Task Write()
//			{
//				var counterChanges = new List<CounterChanges>();
//				counterData.ForEach(keyValue =>
//				{
//					var newCounterChange =
//						new CounterChanges
//						{
//							FullCounterName = keyValue.Key,
//							Delta = keyValue.Value
//						};
//					counterChanges.Add(newCounterChange);
//				});
//
//				var requestUriString = String.Format("{0}/batch", counterStorageUrl);
//
//				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, "POST", credentials, convention)))
//				{
//					try
//					{
//						await request.WriteAsync(RavenJObject.FromObject(counterChanges));
//						var response = await request.ReadResponseJsonAsync();
//					}
//					catch (Exception e)
//					{
//						throw e;
//						//throw e.TryThrowBetterError();
//					}
//				}
//			}
//		}

		#endregion
	}
}