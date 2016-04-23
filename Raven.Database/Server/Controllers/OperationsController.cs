using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    [RoutePrefix("")]
    public class OperationsController : BaseDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("operation/status")]
        [RavenRoute("databases/{databaseName}/operation/status")]
        public HttpResponseMessage OperationStatusGet()
        {
            var idStr = GetQueryStringValue("id");
            long id;
            if (long.TryParse(idStr, out id) == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "Query string variable id must be a valid int64"
                }, HttpStatusCode.BadRequest);
            }

            var status = Database.Tasks.GetTaskState(id);
            return GetOperationStatusMessage(status);
        }

        [HttpGet]
        [RavenRoute("operation/kill")]
        [RavenRoute("databases/{databaseName}/operation/kill")]
        public HttpResponseMessage OperationKill()
        {
            var idStr = GetQueryStringValue("id");
            long id;
            if (long.TryParse(idStr, out id) == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "Query string variable id must be a valid int64"
                }, HttpStatusCode.BadRequest);
            }
            var status = Database.Tasks.KillTask(id);
            return GetOperationStatusMessage(status);
        }

        [HttpGet]
        [RavenRoute("operations")]
        [RavenRoute("databases/{databaseName}/operations")]
        public HttpResponseMessage CurrentOperations()
        {
            return GetMessageWithObject(Database.Tasks.GetAll());
        }

        [HttpGet]
        [RavenRoute("operation/alerts")]
        [RavenRoute("databases/{databaseName}/operation/alerts")]
        public HttpResponseMessage Alerts()
        {
            var jsonDocument = Database.Documents.Get(Constants.RavenAlerts, null);
            if (jsonDocument == null)
            {
                return GetMessageWithObject(new Alert[0]);
            }

            var alerts = jsonDocument.DataAsJson.JsonDeserialization<AlertsDocument>();
            if (alerts == null)
            {
                return GetMessageWithObject(new Alert[0]);
            }
            var now = SystemTime.UtcNow;
            var filteredAlerts = alerts.Alerts.Where(
                a => a.Observed == false &&
                    (a.LastDismissedAt.HasValue == false || a.LastDismissedAt.Value.AddDays(1) < now))
                .ToList();

            return GetMessageWithObject(filteredAlerts);
        }

        [HttpPost]
        [RavenRoute("operation/alert/dismiss")]
        [RavenRoute("databases/{databaseName}/operation/alert/dismiss")]
        public async Task<HttpResponseMessage> AlertDismiss()
        {
            var request = await ReadJsonObjectAsync<RavenJObject>().ConfigureAwait(false);
            var key = request.Value<string>("key");
            var jsonDocument = Database.Documents.Get(Constants.RavenAlerts, null);
            if (jsonDocument == null)
            {
                return GetMessageWithString("Unable to find Raven/Alerts document", HttpStatusCode.BadRequest);
            }

            var alerts = jsonDocument.DataAsJson.JsonDeserialization<AlertsDocument>();
            var alertToDismiss = alerts.Alerts.FirstOrDefault(alert => alert.UniqueKey == key);
            if (alertToDismiss == null)
            {
                return GetMessageWithString("Unable to find alert with key: " + key, HttpStatusCode.BadRequest);
            }
            alertToDismiss.Observed = true;
            alertToDismiss.LastDismissedAt = SystemTime.UtcNow;

            Database.Documents.Put(Constants.RavenAlerts, null, RavenJObject.FromObject(alerts), jsonDocument.Metadata, null);
            return GetEmptyMessage();
        }

        private HttpResponseMessage GetOperationStatusMessage(IOperationState status) 
        {
            if (status == null)
            {
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }


            if (status.State != null)
            {
                lock (status.State)
                {
                    return GetMessageWithObject(status);
                }
            }
            return GetMessageWithObject(status);
        }
    }
};
