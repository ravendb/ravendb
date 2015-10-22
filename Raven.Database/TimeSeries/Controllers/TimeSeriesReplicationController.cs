using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.TimeSeries;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.TimeSeries.Controllers
{
    public class TimeSeriesReplicationController : BaseTimeSeriesApiController
    {
		[RavenRoute("ts/{timeSeriesName}/lastEtag")]
		[HttpGet]
		public HttpResponseMessage GetLastEtag(Guid serverId)
		{
			using (var reader = TimeSeries.CreateReader())
			{
				var lastEtag = reader.GetLastEtag();
				return Request.CreateResponse(HttpStatusCode.OK, lastEtag);
			}
		}

        [RavenRoute("ts/{timeSeriesName}/replication")]
		[HttpPost]
		public async Task<HttpResponseMessage> Post()
		{
			TimeSeries.MetricsTimeSeries.IncomingReplications.Mark();

			ReplicationMessage replicationMessage;
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
			/*using (var writer = TimeSeries.CreateWriter())
			{
				var counterChangeNotifications = new List<ChangeNotification>();
				foreach (var counter in replicationMessage.TimeSeries)
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

					using (var reader = TimeSeriestorage.CreateReader())
					{
						counterChangeNotifications.ForEach(change =>
						{
							change.Total = reader.GetCounterTotal(change.GroupName, change.CounterName);
							TimeSeriestorage.Publisher.RaiseNotification(change);
						});
					}
				}

				return GetEmptyMessage();
			}*/
	        throw new NotImplementedException();
		}

		[RavenRoute("ts/{timeSeriesName}/replication/heartbeat")]
		[HttpPost]
        public HttpResponseMessage HeartbeatPost(string sourceServer)
        {
			var replicationTask = TimeSeries.ReplicationTask;
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

		[RavenRoute("ts/{timeSeriesName}/replications/get")]
		[HttpGet]
		public HttpResponseMessage ReplicationsGet()
		{
			using (var reader = TimeSeries.CreateReader())
			{
				var replicationData = reader.GetReplicationData();

				if (replicationData == null || replicationData.Destinations.Count == 0)
				{
					return GetEmptyMessage(HttpStatusCode.NotFound);
				}
				return GetMessageWithObject(replicationData);
			}
		}

		[RavenRoute("ts/{timeSeriesName}/replications/save")]
		[HttpPost]
		public async Task<HttpResponseMessage> ReplicationsSave()
		{
			var newReplicationDocument = await ReadJsonObjectAsync<TimeSeriesReplicationDocument>().ConfigureAwait(false);
			using (var writer = TimeSeries.CreateWriter())
			{
				writer.UpdateReplications(newReplicationDocument);
				writer.Commit();

				return GetEmptyMessage();
			}
		}
    }
}