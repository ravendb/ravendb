using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Replication;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Counters.Controllers
{
    public class CounterReplicationController : RavenCountersApiController
    {
		[RavenRoute("counters/{counterName}/lastEtag")]
		[HttpGet]
		public HttpResponseMessage GetLastEtag(string serverUrl)
		{
			using (var reader = Storage.CreateReader())
			{
				var sourceId = reader.SourceIdFor(serverUrl);
				var result = reader.GetServerEtags().FirstOrDefault(x => x.SourceId == sourceId) ?? new CounterStorage.ServerEtag();
				return Request.CreateResponse(HttpStatusCode.OK, result.Etag);
			}
		}

        [RavenRoute("counters/{counterName}/replication")]
		[HttpPost]
        public async Task<HttpResponseMessage> Post()
        {
            /*Read Current Counter Value for CounterStorageName - Need ReaderWriter Lock
             *If values are ABS larger
             *      Write delta
             *Store last ETag for servers we've successfully rpelicated to
             */
			ReplicationMessage replicationMessage;
            Storage.MetricsCounters.IncomingReplications.Mark();

			try
			{
				replicationMessage = await ReadJsonObjectAsync<ReplicationMessage>();
			}
			catch (Exception e)
			{
				return Request.CreateResponse(HttpStatusCode.BadRequest, e.Message);
			}

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
                        Counter.PerServerValue currentServerValue;
		                if (currentCounter != null)
		                {
				            currentServerValue = currentCounter.ServerValues
								.FirstOrDefault(x => x.SourceId == writer.SourceIdFor(serverValue.ServerName)) ??
				                                    new Counter.PerServerValue
				                                    {
					                                    Negative = 0,
					                                    Positive = 0,
				                                    };

			                // old update, have updates after it already
		                    if (serverValue.Positive <= currentServerValue.Positive &&
		                        serverValue.Negative <= currentServerValue.Negative)
		                        continue;
		                }
		                else
		                {
		                    currentServerValue = new Counter.PerServerValue
		                    {
		                        Negative = 0,
                                Positive = 0
		                    };
		                }

		                wroteCounter = true;
			            writer.Store(serverValue.ServerName,
				            counter.CounterName,
				            Math.Max(serverValue.Positive, currentServerValue.Positive),
				            Math.Max(serverValue.Negative, currentServerValue.Negative)
				        );
		            }
	            }

				var sendingServerName = replicationMessage.SendingServerName;
				if (wroteCounter || writer.GetLastEtagFor(sendingServerName) < lastEtag)
                {
					writer.RecordLastEtagFor(sendingServerName, lastEtag);
                    writer.Commit(); 
                }

	            return new HttpResponseMessage(HttpStatusCode.OK);
            }
        }

        [RavenRoute("counters/{counterName}/replication/heartbeat")]
		[HttpPost]
        public HttpResponseMessage HeartbeatPost(string sourceServer)
        {
            var replicationTask = Storage.ReplicationTask;
            if (replicationTask == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "Cannot find replication task setup in the database"
                }, HttpStatusCode.NotFound);

            }

			replicationTask.HandleHeartbeat(sourceServer);

            return GetEmptyMessage();
        }

		[RavenRoute("counters/{counterName}/replications/get")]
		[HttpGet]
		public HttpResponseMessage ReplicationsGet()
		{
			using (var reader = Storage.CreateReader())
			{
				var replicationData = reader.GetReplicationData();

				if (replicationData == null || replicationData.Destinations.Count == 0)
				{
					return Request.CreateResponse(HttpStatusCode.NotFound);
				}
				return Request.CreateResponse(HttpStatusCode.OK, replicationData);
			}
		}

		[RavenRoute("counters/{counterName}/replications/save")]
		[HttpPost]
		public async Task<HttpResponseMessage> ReplicationsSave()
		{
			var newReplicationDocument = await ReadJsonObjectAsync<CounterStorageReplicationDocument>();
			using (var writer = Storage.CreateWriter())
			{
				writer.UpdateReplications(newReplicationDocument);
				writer.Commit();

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

	    [RavenRoute("counters/{counterName}/replications/stats")]
        [HttpGet]
        public HttpResponseMessage ReplicationStats()
        {
            return Request.CreateResponse(HttpStatusCode.OK,
                new CounterStorageReplicationStats()
                {
                    Stats = Storage.ReplicationTask.DestinationStats.Values.ToList()
                });
        }
    }
}