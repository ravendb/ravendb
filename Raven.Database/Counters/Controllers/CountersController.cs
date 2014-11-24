using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;

namespace Raven.Database.Counters.Controllers
{
	public class CountersController : RavenCountersApiController
	{
		[Route("counters/{counterName}/change")]
		[HttpPost]
		public HttpResponseMessage CounterChange(string group, string counterName, long delta)
		{
			using (var writer = Storage.CreateWriter())
			{
				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				writer.Store(Storage.CounterStorageUrl, counterFullName, delta);
				writer.Commit(delta != 0);
                Storage.MetricsCounters.ClientRequests.Mark();
				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[Route("counters/{counterName}/batch")]
		[HttpPost]
		public async Task<HttpResponseMessage> CountersBatch()
		{
			List<CounterChanges> counterChanges;
			try
			{
				counterChanges = await ReadJsonObjectAsync<List<CounterChanges>>();
			}
			catch (Exception e)
			{
				return Request.CreateResponse(HttpStatusCode.BadRequest, e.Message);
			}

			using (var writer = Storage.CreateWriter())
			{
				counterChanges.ForEach(counterChange => 
					writer.Store(Storage.CounterStorageUrl, counterChange.FullCounterName, counterChange.Delta));

				return new HttpResponseMessage(HttpStatusCode.OK);
			}	
		}

		[Route("counters/{counterName}/reset")]
		[HttpPost]
		public HttpResponseMessage CounterReset(string group, string counterName)
		{
			using (var writer = Storage.CreateWriter())
			{
				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				bool changesWereMade = writer.Reset(Storage.CounterStorageUrl, counterFullName);

				if (changesWereMade)
				{
					writer.Commit();
				}
				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[Route("counters/{counterName}/groups")]
		[HttpGet]
		public HttpResponseMessage GetCounterGroups()
		{
			using (var reader = Storage.CreateReader())
			{
				return Request.CreateResponse(HttpStatusCode.OK, reader.GetCounterGroups().ToList());
			}
		}

		[Route("counters/{counterName}/counters")]
		[HttpGet]
		public HttpResponseMessage GetCounters(int skip = 0, int take = 20, string group = null)
		{
			using (var reader = Storage.CreateReader())
			{
				var prefix = (group == null) ? string.Empty : (group + Constants.GroupSeperatorString);
				var results = (
					from counterFullName in reader.GetCounterNames(prefix)
					let counter = reader.GetCounter(counterFullName)
					select new CounterView
					{
						Name = counterFullName.Split(Constants.GroupSeperatorChar)[1],
						Group = counterFullName.Split(Constants.GroupSeperatorChar)[0],
						OverallTotal = counter.ServerValues.Sum(x => x.Positive - x.Negative),

						Servers = counter.ServerValues.Select(s => new CounterView.ServerValue
						{
							Negative = s.Negative, Positive = s.Positive, Name = reader.ServerNameFor(s.SourceId)
						}).ToList()
					}).ToList();
				return Request.CreateResponse(HttpStatusCode.OK, results);
			}
		}

        [Route("counters/{counterName}/getCounterOverallTotal")]
        [HttpGet]
		public HttpResponseMessage GetCounterOverallTotal(string group, string counterName)
        {
			using (var reader = Storage.CreateReader())
			{
				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				Counter counter = reader.GetCounter(counterFullName);
				
				if (counter == null)
				{
					return Request.CreateResponse(HttpStatusCode.NotFound);
				}

				long overallTotal = counter.ServerValues.Sum(x => x.Positive - x.Negative);
				return Request.CreateResponse(HttpStatusCode.OK, overallTotal);
			}
        }

        [Route("counters/{counterName}/getCounterServersValues")]
        [HttpGet]
        public HttpResponseMessage GetCounterServersValues(string group, string counterName)
        {
            using (var reader = Storage.CreateReader())
            {
                string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
                Counter counter = reader.GetCounter(counterFullName);

                if (counter == null)
                {
                    return Request.CreateResponse(HttpStatusCode.NotFound);
                }

                List<CounterView.ServerValue> serverValues =
                    counter.ServerValues.Select(s => new CounterView.ServerValue
                    {
                        Negative = s.Negative,
                        Positive = s.Positive,
                        Name = reader.ServerNameFor(s.SourceId)
                    }).ToList();
                return Request.CreateResponse(HttpStatusCode.OK, serverValues);
            }
        }

        [Route("counters/{counterName}/metrics")]
        [HttpGet]
        public HttpResponseMessage CounterMetrics()
        {
            return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateMetrics());            
        }

        [Route("counters/{counterName}/stats")]
        [HttpGet]
        public HttpResponseMessage CounterStats()
        {
            return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateStats());
        }
	}
}