using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.TimeSeries;
using Raven.Database.Counters;
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