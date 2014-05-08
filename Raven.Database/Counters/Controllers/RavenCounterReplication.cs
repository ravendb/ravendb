using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Counters.Controllers
{
	public class RavenCounterReplication
	{
		public static string GetServerNameForWire(string server)
		{
			var uri = new Uri(server);
			return uri.Host + ":" + uri.Port;
		}

		private readonly CounterStorage storage;
		private readonly CancellationTokenSource cancellation;

		public RavenCounterReplication(CounterStorage storage)
		{
			this.storage = storage;
			this.storage.CounterUpdated += workContext_CounterUpdated;
			cancellation = new CancellationTokenSource();
		}

		void workContext_CounterUpdated()
		{
			if (!cancellation.IsCancellationRequested)
				Replicate();
		}

		public void ShutDown()
		{
			cancellation.Cancel();
		}

		public async void Replicate()
		{
			var tasks =
				storage.Servers
					.Where(x => x != storage.Name) //skip "this" server
					.Select(async server =>
					{
						var http = new HttpClient();
						var etagResult = await http.GetStringAsync(string.Format("{0}/lastEtag/{1}", server, GetServerNameForWire(storage.Name)));
						var etag = int.Parse(etagResult);
						var message = new ReplicationMessage {SendingServerName = storage.Name};
						using (var reader = storage.CreateReader())
						{
							message.Counters = reader.GetCountersSinceEtag(etag).Take(1024).ToList(); //TODO: Capped this...how to get remaining values?
						}
						var url = string.Format("{0}/replication", server);
						return message.Counters.Count > 0 ?
							new HttpClient().PostAsync(url, message, new JsonMediaTypeFormatter()) :
							Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotModified)); //HACK: could do something else here
					});

			try
			{
				await Task.WhenAll(tasks);
			}
			catch (Exception)
			{
                
				//TODO: log
			}   
		}
	}
}