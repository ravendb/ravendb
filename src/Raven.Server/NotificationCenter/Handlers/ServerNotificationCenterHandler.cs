using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Util;
using Raven.Server.Dashboard;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerNotificationCenterHandler : ServerNotificationHandlerBase
    {
        [RavenAction("/server/notification-center/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var isValidFor = GetDatabaseAccessValidationFunc();
                using (var writer = new NotificationCenterWebSocketWriter(webSocket, ServerStore.NotificationCenter, ServerStore.ContextPool, ServerStore.ServerShutdown))
                {
                    using (ServerStore.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
                    {
                        foreach (var action in storedNotifications)
                        {
                            if (isValidFor != null)
                            {
                                if (action.Json.TryGet("Database", out string db) == false ||
                                    isValidFor(db, false) == false)
                                    continue; // not valid for this, skipping
                            }

                            await writer.WriteToWebSocket(action.Json);
                        }
                    }

                    foreach (var operation in ServerStore.Operations.GetActive().OrderBy(x => x.Description.StartTime))
                    {
                        var action = OperationChanged.Create(null, operation.Id, operation.Description, operation.State, operation.Killable);

                        await writer.WriteToWebSocket(action.ToJson());
                    }

                    // update the connection with the current cluster topology
                    writer.AfterTrackActionsRegistration = ServerStore.NotifyAboutClusterTopologyAndConnectivityChanges;

                    await writer.WriteNotifications(isValidFor);
                }
            }
        }

        [RavenAction("/server/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task DismissPost()
        {
            var id = GetStringQueryString("id");

            var forever = GetBoolValueQueryString("forever", required: false);
            var dbForId = ServerStore.NotificationCenter.GetDatabaseFor(id);
            var isValidFor = GetDatabaseAccessValidationFunc();
            if (isValidFor != null && isValidFor(dbForId, true) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                return HttpContext.Response
                    .WriteAsync("{'Error':'Invalid attempt to dismiss notification that you do not have access for'}");
            }
            if (forever == true)
                ServerStore.NotificationCenter.Postpone(id, DateTime.MaxValue);
            else
                ServerStore.NotificationCenter.Dismiss(id);

            return NoContent();
        }

        [RavenAction("/server/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");
            var dbForId = ServerStore.NotificationCenter.GetDatabaseFor(id);
            var isValidFor = GetDatabaseAccessValidationFunc();
            if (isValidFor != null && isValidFor(dbForId, true) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                return HttpContext.Response
                    .WriteAsync("{'Error':'Invalid attempt to postpone notification that you do not have access for'}");
            }

            var until = timeInSec == 0 ? DateTime.MaxValue : SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec));
            ServerStore.NotificationCenter.Postpone(id, until);

            return NoContent();
        }
    }

    public abstract class ServerNotificationHandlerBase : RequestHandler
    {
        protected CanAccessDatabase GetDatabaseAccessValidationFunc()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return null;
                    return (_, __) => false; // deny everything

                case RavenServer.AuthenticationStatus.Operator:
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return null;

                case RavenServer.AuthenticationStatus.Allowed:
                    return (database, requireWrite) =>
                    {
                        switch (database)
                        {
                            case null:
                                return false;

                            case "*":
                                return true;

                            default:
                                return feature.CanAccess(database, requireAdmin: false, requireWrite: requireWrite);
                        }
                    };
                default:
                    ThrowInvalidFeatureStatus(status.Value);
                    return null; // never hit
            }
        }

        private static void ThrowInvalidFeatureStatus(RavenServer.AuthenticationStatus status)
        {
            throw new ArgumentOutOfRangeException("Unknown feature status: " + status);
        }
    }
}
