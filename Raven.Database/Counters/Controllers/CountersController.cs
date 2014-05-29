using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
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
				string counter = String.Join(Constants.GroupSeperatorString, new [] { group, counterName });
				writer.Store(Storage.CounterStorageUrl, counter, delta);

				writer.Commit(delta != 0);
				return new HttpResponseMessage(HttpStatusCode.Accepted);
			}
		}

		[Route("counters/{counterName}/reset")]
		[HttpPost]
		public HttpResponseMessage CounterReset(string counterName, string group)
		{
			using (var writer = Storage.CreateWriter())
			{
				string counter = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				bool changesWereMade = writer.Reset(Storage.CounterStorageUrl, counter);

				if (changesWereMade)
				{
					writer.Commit();
				}
				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[Route("counters/{counterName}/groups")]
		[HttpGet]
		public HttpResponseMessage Groups()
		{
			using (var reader = Storage.CreateReader())
			{
				return Request.CreateResponse(HttpStatusCode.OK, reader.GetCounterGroups().ToList());
			}
		}

		[Route("counters/{counterName}/counters")]
		[HttpGet]
		public HttpResponseMessage Counters(int skip = 0, int take = 20, string counterGroupName = null)
		{
            // todo:change counterGroupName to "group"
			using (var reader = Storage.CreateReader())
			{
				var prefix = (counterGroupName == null) ? string.Empty : (counterGroupName + Constants.GroupSeperatorString);
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

        [Route("counters/{counterName}/getCounterValue")]
        [HttpGet]
        public HttpResponseMessage GetCounterValue(string counterName, string group = null)
        {
            using (var reader = Storage.CreateReader())
            {
                var prefix = (group == null) ? string.Empty : (group + Constants.GroupSeperatorString) + counterName;
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
                            Negative = s.Negative,
                            Positive = s.Positive,
                            Name = reader.ServerNameFor(s.SourceId)
                        }).ToList()
                    }).ToList();
                return Request.CreateResponse(HttpStatusCode.OK, results);
            }
        }

		public class CounterView
		{
			public string Name { get; set; }
			public string Group { get; set; }
			public long OverallTotal { get; set; }
			public List<ServerValue> Servers { get; set; }


			public class ServerValue
			{
				public string Name { get; set; }
				public long Positive { get; set; }
				public long Negative { get; set; }
			}
		}
	}
}