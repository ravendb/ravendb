using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Counters.Controllers
{
	public class CountersController : RavenCountersApiController
	{
		[Route("counters/{counterName}/change")]
		[HttpGet]
		public HttpResponseMessage Change(string counter, long delta)
		{
			using (var writer = Storage.CreateWriter())
			{
				writer.Store(Storage.Name,counter, delta);

				writer.Commit();
				return new HttpResponseMessage(HttpStatusCode.Accepted);
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
		public HttpResponseMessage Counters()
		{
			using (var reader = Storage.CreateReader())
			{
				var results = (
					from counterName in reader.GetCounterNames(string.Empty)
					let counter = reader.GetCounter(counterName)
					select new CounterView
					{
						Name = counterName,
						OverallTotal = counter.ServerValues.Sum(x => x.Positive - x.Negative),
						Servers = counter.ServerValues.Select(s => new CounterView.ServerValue
						{
							Negative = s.Negative, Positive = s.Positive, Name = Storage.ServerNameFor(s.SourceId)
						}).ToList()
					}).ToList();
				return Request.CreateResponse(HttpStatusCode.OK, results);
			}
		}


		public class CounterView
		{
			public string Name { get; set; }
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