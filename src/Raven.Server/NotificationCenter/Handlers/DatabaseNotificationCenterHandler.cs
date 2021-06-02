using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/notification-center/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebSocketWriter(webSocket, Database.NotificationCenter, ContextPool, Database.DatabaseShutdown))
                {
                    using (Database.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
                    {
                        foreach (var alert in storedNotifications)
                        {
                            await writer.WriteToWebSocket(alert.Json);
                        }
                    }

                    foreach (var operation in Database.Operations.GetActive().OrderBy(x => x.Description.StartTime))
                    {
                        var action = OperationChanged.Create(Database.Name, operation.Id, operation.Description, operation.State, operation.Killable);

                        await writer.WriteToWebSocket(action.ToJson());
                    }
                    writer.AfterTrackActionsRegistration = ServerStore.NotifyAboutClusterTopologyAndConnectivityChanges;
                    await writer.WriteNotifications(null);
                }
            }
        }

        [RavenAction("/databases/*/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task DismissPost()
        {
            var id = GetStringQueryString("id");
            var forever = GetBoolValueQueryString("forever", required: false);

            if (forever == true)
                Database.NotificationCenter.Postpone(id, DateTime.MaxValue);
            else
                Database.NotificationCenter.Dismiss(id);

            return NoContent();
        }

        [RavenAction("/databases/*/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            var until = timeInSec == 0 ? DateTime.MaxValue : SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec));
            Database.NotificationCenter.Postpone(id, until);

            return NoContent();
        }
    }
}
