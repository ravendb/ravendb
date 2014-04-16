using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Counters.Controllers
{
    public class CounterReplicationController : RavenCountersApiController
    {
        [Route("counters/replication")]
        public HttpResponseMessage Post(ReplicationMessage replicationMessage)
        {
            /*Read Current Counter Value for CounterName - Need ReaderWriter Lock
             *If values are ABS larger
             *      Write delta
             *Store last ETag for servers we've successfully rpelicated to
             */
	        long lastEtag = 0;
            using (var writer = Storage.CreateWriter())
            {
	            foreach (var counter in replicationMessage.Counters)
	            {
		            lastEtag = Math.Max(counter.Counter.Etag, lastEtag);
		            var currentCounter = writer.GetCounter(counter.CounterName);
		            foreach (var serverValue in counter.Counter.ServerValues)
		            {
			            var currentServerValue = currentCounter.ServerValues
				            .FirstOrDefault(x => x.SourceId == serverValue.SourceId) ??
			                                     new Counter.PerServerValue
			                                     {
				                                     Negative = 0,
				                                     Positive = 0,
			                                     };

			            if (serverValue.Positive == currentServerValue.Positive &&
			                serverValue.Negative == currentServerValue.Negative)
				            continue;

			            writer.Store(replicationMessage.SendingServerName,
				            counter.CounterName,
				            Math.Max(serverValue.Positive, currentServerValue.Positive),
				            Math.Max(serverValue.Negative, currentServerValue.Negative)
				            );
		            }
	            }
	            
				writer.RecordLastEtagFor(replicationMessage.SendingServerName, lastEtag);

	            writer.Commit();
	            return new HttpResponseMessage(HttpStatusCode.OK);
            }
        }

        [Route("counters/{server}/lastEtag")]
        public HttpResponseMessage GetLastEtag(string server)
        {
            var sourceId = Storage.SourceIdFor(server);
            using (var reader = Storage.CreateReader())
            {
                var result = reader.GetServerEtags().FirstOrDefault(x => x.SourceId == sourceId) ?? new CounterStorage.ServerEtag();
                return Request.CreateResponse(HttpStatusCode.OK, result.Etag);
            }
        }
    }

    public class CounterWorkContext
    {
        public event Action CounterUpdated = () => { };
    }
    //bah - this is not a controller, but I need access to storage. move that around later.
    public class RavenCounterReplication : RavenCountersApiController
    {
        private readonly CancellationTokenSource cancellation;
        private long lastEtag;

        RavenCounterReplication(CounterWorkContext workContext)
        {
            workContext.CounterUpdated += workContext_CounterUpdated;
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
                Storage.Servers
                    .Select(async server =>
                    {
                        var http = new HttpClient();
                        var etagResult = await http.GetStringAsync(string.Format("{0}/counters/{0}/lastEtag", server));
                        var etag = int.Parse(etagResult);
                        var message = new ReplicationMessage {SendingServerName = Storage.Name};
                        using (var reader = Storage.CreateReader())
                        {
                           message.Counters = reader.GetCountersSinceEtag(etag).Take(1024).ToList(); //TODO: Capped this...how to get remaining values?
                        }
                        var url = string.Format("{0}/counters/replication", server);
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