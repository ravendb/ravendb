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
            using (var writer = Storage.CreateWriter())
            {
                foreach (var counter in replicationMessage.Counters)
                {
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
                
                writer.Commit();
                return new HttpResponseMessage(HttpStatusCode.OK);
            }
        }   
    }

    //bah - this is not a controller, but I need access to storage. move that around later.
    public class RavenCounterReplication : RavenCountersApiController
    {
        private static readonly CancellationTokenSource Cancellation;
        private static long lastEtag;

        static RavenCounterReplication()
        {
            Cancellation = new CancellationTokenSource();
        }

        public static void Initialize()
        {
            Task.Factory.StartNew(() =>
            {
                var replication = new RavenCounterReplication();
                while (true)
                {
                    if (Cancellation.IsCancellationRequested) break;
                    replication.Replicate();
                    Thread.Sleep(100); //TODO: something better
                }
            }, TaskCreationOptions.LongRunning);
        }

        public static void ShutDown()
        {
            Cancellation.Cancel();
        }

        public void Replicate()
        {
            if (Storage.LastEtag <= lastEtag) return;
            var nextEtag = Storage.LastEtag;
            
            Counter serverEtags;
            List<ReplictionCounter> counters;
            const string key = "raven/internal/counters/server-etags";
            using (var reader = Storage.CreateReader())
            {
                serverEtags = reader.GetCounter(key);
                var min = serverEtags.ServerValues.Min(x => x.Positive);
                counters = reader.GetCountersSinceEtag(min).ToList();
            }

            
            var servers = Storage.Servers
                            .Select(server =>
                            {
                                var serverValue = serverEtags.ServerValues.FirstOrDefault(x => Storage.ServerNameFor(x.SourceId) == server);
                                return new
                                {
                                    Name = server,
                                    LastEtagSent = serverValue == null ? 0 : serverValue.Positive
                                };
                            })
                            .ToArray();

            var tasks =
                servers
                    .Select(server => new 
                    {
                        ServerName = server.Name,
                        ReplicationMessage =  new ReplicationMessage
                        {
                            SendingServerName = Storage.Name,
                            Counters = counters.Where(x => x.Counter.Etag > server.LastEtagSent && x.Counter.Etag <= nextEtag).ToList()
                        }
                    })
                    .Select(x => x.ReplicationMessage.Counters.Count > 0 ?
                        new HttpClient().PostAsync(x.ServerName, x.ReplicationMessage, new JsonMediaTypeFormatter()):
                        Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotModified))) //HACK: could do something else here
                    .ToArray();

            Task.WaitAll(tasks);

            using (var writer = Storage.CreateWriter())
            {
                for (var i = 0; i < tasks.Length; i++)
                {
                    if (tasks[i].Result.IsSuccessStatusCode)
                    {
                        writer.Store(servers[i].Name, key, nextEtag, 0L);
                    }
                }           
            }

            lastEtag = nextEtag;
        }
    }
}