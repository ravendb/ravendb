using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Counters.Controllers
{
    public class CountersReplicationController : RavenCountersApiController
    {
		[RavenRoute("cs/{counterStorageName}/lastEtag")]
		[HttpGet]
		public HttpResponseMessage GetLastEtag(Guid serverId)
		{
			using (var reader = Storage.CreateReader())
			{
				var lastEtag = reader.GetLastEtagFor(serverId);
				return Request.CreateResponse(HttpStatusCode.OK, lastEtag);
			}
		}

        [RavenRoute("cs/{counterStorageName}/replication")]
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
				var counterChangeNotifications = new List<ReplicationChangeNotification>();
	            foreach (var counter in replicationMessage.Counters)
	            {
		            lastEtag = Math.Max(counter.Etag, lastEtag);
					var currentCounter = writer.GetCounterValue(counter.FullCounterName);

					//if current counter exists and current value is less than received value
		            if (currentCounter != -1 && currentCounter <= counter.Value)
						continue;

					wroteCounter = true;

					if (string.IsNullOrWhiteSpace(counter.FullCounterName))
						return Request.CreateResponse(HttpStatusCode.BadRequest, "Invalid or empty counter name.");
					
					var counterValue = new CounterValue(counter.FullCounterName, counter.Value);
					var counterChangeAction = writer.Store(counterValue);
					counterChangeNotifications.Add(new ReplicationChangeNotification
					{
						GroupName = counterValue.Group(),
						CounterName = counterValue.CounterName(),
						Action = counterChangeAction,
					});
				}

				var serverId = replicationMessage.ServerId;
	            //if (String.IsNullOrWhiteSpace(serverId))
		        //    return Request.CreateResponse(HttpStatusCode.BadRequest, "Invalid or empty server Id");

				if (wroteCounter || writer.GetLastEtagFor(serverId) < lastEtag)
                {
					writer.RecordLastEtagFor(serverId, lastEtag);
                    writer.Commit();

					counterChangeNotifications.ForEach(Storage.Publisher.RaiseNotification);
                }

	            return new HttpResponseMessage(HttpStatusCode.OK);
            }
        }

        [RavenRoute("cs/{counterStorageName}/replication/heartbeat")]
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

		[RavenRoute("cs/{counterStorageName}/replications/get")]
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

		[RavenRoute("cs/{counterStorageName}/replications/save")]
		[HttpPost]
		public async Task<HttpResponseMessage> ReplicationsSave()
		{
			var newReplicationDocument = await ReadJsonObjectAsync<CountersReplicationDocument>();
			using (var writer = Storage.CreateWriter())
			{
				writer.UpdateReplications(newReplicationDocument);
				writer.Commit();

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}
    }
}