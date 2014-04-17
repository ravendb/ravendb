using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;

namespace Raven.Database.Counters.Controllers
{
    public class CounterReplicationController : RavenCountersApiController
    {
        [Route("counters/{counterName}/replication")]
        public HttpResponseMessage Post(ReplicationMessage replicationMessage)
        {
            /*Read Current Counter Value for CounterName - Need ReaderWriter Lock
             *If values are ABS larger
             *      Write delta
             *Store last ETag for servers we've successfully rpelicated to
             */
	        long lastEtag = 0;
            bool wroteCounter = false;
            using (var writer = Storage.CreateWriter())
            {
	            foreach (var counter in replicationMessage.Counters)
	            {
		            lastEtag = Math.Max(counter.Etag, lastEtag);
		            var currentCounter = writer.GetCounter(counter.CounterName);
		            foreach (var serverValue in counter.ServerValues)
		            {
			            var currentServerValue = currentCounter.ServerValues
				            .FirstOrDefault(x => x.SourceId == Storage.SourceIdFor(serverValue.ServerName)) ??
			                                     new Counter.PerServerValue
			                                     {
				                                     Negative = 0,
				                                     Positive = 0,
			                                     };

			            if (serverValue.Positive == currentServerValue.Positive &&
			                serverValue.Negative == currentServerValue.Negative)
				            continue;

		                wroteCounter = true;
			            writer.Store(replicationMessage.SendingServerName,
				            counter.CounterName,
				            Math.Max(serverValue.Positive, currentServerValue.Positive),
				            Math.Max(serverValue.Negative, currentServerValue.Negative)
				            );
		            }
	            }

                if (wroteCounter)
                {
                    writer.RecordLastEtagFor(replicationMessage.SendingServerName, lastEtag);
                    writer.Commit(); 
                }
	            return new HttpResponseMessage(HttpStatusCode.OK);
            }
        }

        [Route("counters/{counterName}/lastEtag/{server}")]
        public HttpResponseMessage GetLastEtag(string server)
        {
            server = Storage.Name.Replace(RavenCounterReplication.GetServerNameForWire(Storage.Name), server); //HACK: nned a wire firendly name or fix Owin impl to allow url on query string or just send back all etags
            var sourceId = Storage.SourceIdFor(server);
            using (var reader = Storage.CreateReader())
            {
                var result = reader.GetServerEtags().FirstOrDefault(x => x.SourceId == sourceId) ?? new CounterStorage.ServerEtag();
                return Request.CreateResponse(HttpStatusCode.OK, result.Etag);
            }
        }
    }

    public class RavenCounterReplication
    {
        public static string GetServerNameForWire(string server)
        {
            var uri = new Uri(server);
            return uri.Host + ":" + uri.Port;
        }

        private readonly CounterStorage storage;
        private readonly CancellationTokenSource cancellation;
        private long lastEtag;

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