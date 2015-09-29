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
    public class CountersReplicationController : BaseCountersApiController
    {
		[RavenRoute("cs/{counterStorageName}/lastEtag")]
		[HttpGet]
		public HttpResponseMessage GetLastEtag(Guid serverId)
		{
			using (var reader = CounterStorage.CreateReader())
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
			CounterStorage.MetricsCounters.IncomingReplications.Mark();

			try
			{
				replicationMessage = await ReadJsonObjectAsync<ReplicationMessage>().ConfigureAwait(false);
			}
			catch (Exception e)
			{
				return Request.CreateResponse(HttpStatusCode.BadRequest, e.Message);
			}

	        long lastEtag = 0;
            var wroteCounter = false;
            using (var writer = CounterStorage.CreateWriter())
            {
				var counterChangeNotifications = new List<ChangeNotification>();
	            foreach (var counter in replicationMessage.Counters)
				{					
		            lastEtag = Math.Max(counter.Etag, lastEtag);
					var singleCounterValue = writer.GetSingleCounterValue(counter.GroupName, counter.CounterName, counter.ServerId, counter.Sign);
					var currentCounterValue = singleCounterValue.Value;

					//if current counter exists and current value is less than received value
					if ((currentCounterValue != -1 && counter.Value < currentCounterValue) ||
						(counter.Value == currentCounterValue && (singleCounterValue.DoesCounterExist || writer.IsTombstone(counter.ServerId))))
						continue;

					wroteCounter = true;
					var counterChangeAction = writer.Store(counter.GroupName, counter.CounterName, counter.ServerId, counter.Sign, counter.Value);

					counterChangeNotifications.Add(new ChangeNotification
					{
						GroupName = counter.GroupName,
						CounterName = counter.CounterName,
						Delta = counter.Value - currentCounterValue,
						Action = counterChangeAction
					});
				}

				var serverId = replicationMessage.ServerId;
				if (wroteCounter || writer.GetLastEtagFor(serverId) < lastEtag)
                {
					writer.RecordSourceNameFor(serverId, replicationMessage.SendingServerName);
					writer.RecordLastEtagFor(serverId, lastEtag);
                    writer.Commit();

	                using (var reader = CounterStorage.CreateReader())
	                {
		                counterChangeNotifications.ForEach(change =>
		                {
			                change.Total = reader.GetCounterTotal(change.GroupName, change.CounterName);
							CounterStorage.Publisher.RaiseNotification(change);
		                });
	                }
                }

	            return GetEmptyMessage();
            }
        }

        [RavenRoute("cs/{counterStorageName}/replication/heartbeat")]
		[HttpPost]
        public HttpResponseMessage HeartbeatPost(string sourceServer)
        {
			var replicationTask = CounterStorage.ReplicationTask;
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
			using (var reader = CounterStorage.CreateReader())
			{
				var replicationData = reader.GetReplicationData();

				if (replicationData == null || replicationData.Destinations.Count == 0)
				{
					return GetEmptyMessage(HttpStatusCode.NotFound);
				}
				return GetMessageWithObject(replicationData);
			}
		}

		[RavenRoute("cs/{counterStorageName}/replications/save")]
		[HttpPost]
		public async Task<HttpResponseMessage> ReplicationsSave()
		{
			var newReplicationDocument = await ReadJsonObjectAsync<CountersReplicationDocument>().ConfigureAwait(false);
			using (var writer = CounterStorage.CreateWriter())
			{
				writer.UpdateReplications(newReplicationDocument);
				writer.Commit();

				return GetEmptyMessage();
			}
		}
    }
}