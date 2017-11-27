using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerNotificationCenterHandler : ServerNotificationHandlerBase
    {
        [RavenAction("/server/notification-center/watch", "GET", AuthorizationStatus.ValidUser)]
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
                                    isValidFor(db) == false)
                                    continue; // not valid for this, skipping
                            }
                                

                            await writer.WriteToWebSocket(action.Json);
                        }
                    }

                    await writer.WriteNotifications(isValidFor);
                }
            }
        }

     
        [RavenAction("/admin/notification-center/dismiss", "POST", AuthorizationStatus.Operator)]
        public Task DismissPost()
        {
            var id = GetStringQueryString("id");

            var forever = GetBoolValueQueryString("forever", required: false);

            if (forever == true)
                ServerStore.NotificationCenter.Postpone(id, DateTime.MaxValue);
            else
                ServerStore.NotificationCenter.Dismiss(id);

            return NoContent();
        }

        [RavenAction("/admin/notification-center/postpone", "POST", AuthorizationStatus.Operator)]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            ServerStore.NotificationCenter.Postpone(id, SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec)));
            
            return NoContent();
        }
    }

    public abstract class ServerNotificationHandlerBase : RequestHandler
    {
        protected Func<string, bool> GetDatabaseAccessValidationFunc()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return null;
                    return s => false; // deny everything

                case RavenServer.AuthenticationStatus.Operator:
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return null;

                case RavenServer.AuthenticationStatus.Allowed:
                    return database =>
                    {
                        switch (database)
                        {
                            case null:
                                return false;
                            case "*":
                                return true;
                            default:
                                return feature.CanAccess(database, false);
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
