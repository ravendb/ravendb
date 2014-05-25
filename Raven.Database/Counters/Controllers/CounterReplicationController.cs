using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Client.Connection;
using Raven.Json.Linq;

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
            RavenJObject replicationMessageJObject = await ReadJsonAsync();
	        ReplicationMessage replicationMessage = ReplicationMessage.GetReplicationMessage(replicationMessageJObject); 

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
        public async Task<HttpResponseMessage> HeartbeatPost()
        {
            var src = GetQueryStringValue("from");

            var replicationTask = Storage.ReplicationTask;
            if (replicationTask == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "Cannot find replication task setup in the database"
                }, HttpStatusCode.NotFound);

            }

            replicationTask.HandleHeartbeat(src);

            return GetEmptyMessage();
        }

        [Route("counters/{counterName}/lastEtag/{server}")]
        public HttpResponseMessage GetLastEtag(string server)
        {
			//HACK: need a wire friendly name or fix Owin impl to allow url on query string or just send back all etags
            server = Storage.CounterStorageUrl.Replace(RavenCounterReplication.GetServerNameForWire(Storage.CounterStorageUrl), server);
            using (var reader = Storage.CreateReader())
            {
				var sourceId = reader.SourceIdFor(server);
                var result = reader.GetServerEtags().FirstOrDefault(x => x.SourceId == sourceId) ?? new CounterStorage.ServerEtag();
                return Request.CreateResponse(HttpStatusCode.OK, result.Etag);
            }
        }

		[Route("counters/{counterName}/replications-get")]
		[HttpGet]
		public async Task<HttpResponseMessage> ReplicationsGet()
		{
			using (var reader = Storage.CreateReader())
			{
				var replicationData = reader.GetReplicationData();
				var responseCode = (replicationData != null) ? HttpStatusCode.OK : HttpStatusCode.BadRequest;

				return Request.CreateResponse(responseCode, reader.GetReplicationData());
			}
		}

		[Route("counters/{counterName}/replications-save")]
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