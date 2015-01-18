using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	public class CountersBatchCommands : CountersActionsBase
	{
		private readonly ConcurrentDictionary<string, long> _counterData = new ConcurrentDictionary<string, long>();

		internal CountersBatchCommands(CountersClient parent) : base(parent)
		{
		}

		public void Change(string group, string counterName, long delta)
		{
			var counterFullName = String.Join(Constants.GroupSeperatorString, new[] { @group, counterName });
			_counterData.AddOrUpdate(counterFullName, delta, (key, existingVal) => existingVal + delta);
		}

		public void Increment(string group, string counterName)
		{
			Change(@group, counterName, 1);
		}

		public void Decrement(string group, string counterName)
		{
			Change(@group, counterName, -1);
		}

		public async Task Write()
		{
			var counterChanges = new List<CounterChange>();
			_counterData.ForEach(keyValue =>
			{
				var newCounterChange =
					new CounterChange
					{
						Name = keyValue.Key,
						Delta = keyValue.Value
					};
				counterChanges.Add(newCounterChange);
			});

			var requestUriString = String.Format("{0}/batch", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(counterChanges));
					var response = await request.ReadResponseJsonAsync();
				}
				catch (Exception e)
				{
					throw e;
					//throw e.TryThrowBetterError();
				}
			}
		}
	}
}