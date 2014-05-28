using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Counters.Controllers
{
    public class CounterReplicationController : RavenCountersApiController
    {
        [Route("counters/{counterName}/replication")]
        public async Task<HttpResponseMessage> Post()
        {
            /*Read Current Counter Value for CounterStorageName - Need ReaderWriter Lock
             *If values are ABS larger
             *      Write delta
             *Store last ETag for servers we've successfully rpelicated to
             */
			ReplicationMessage replicationMessage;
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

        [Route("counters/{counterName}/replication/heartbeat")]
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

		[Route("counters/{counterName}/lastEtag")]
		public HttpResponseMessage GetLastEtag(string serverUrl)
        {
            using (var reader = Storage.CreateReader())
            {
				var sourceId = reader.SourceIdFor(serverUrl);
                var result = reader.GetServerEtags().FirstOrDefault(x => x.SourceId == sourceId) ?? new CounterStorage.ServerEtag();
                return Request.CreateResponse(HttpStatusCode.OK, result.Etag);
            }
        }

		[Route("counters/{counterName}/replications/get")]
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

		[Route("counters/{counterName}/replications/save")]
		[HttpPost]
		public async Task<HttpResponseMessage> ReplicationsSave()
		{
			CounterStorageReplicationDocument newReplicationDocument;
			try
			{
				newReplicationDocument = await ReadJsonObjectAsync<CounterStorageReplicationDocument>();
			}
			catch (Exception e)
			{
				return Request.CreateResponse(HttpStatusCode.BadRequest, e.Message);
			}

			using (var writer = Storage.CreateWriter())
			{
				writer.UpdateReplications(newReplicationDocument);
				writer.Commit();

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}
    }
}