using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/notifications", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task GetNotifications()
        {
            var postponed = GetBoolValueQueryString("postponed", required: false) ?? true;
            var type = GetStringQueryString("type", required: false);
            var start = GetIntValueQueryString("pageStart", required: false) ?? 0;
            var pageSize = GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;
            
            var shouldFilter = type != null;
            if (shouldFilter && Enum.TryParse(typeof(NotificationType), type, out _) == false)
                throw new ArgumentException($"The 'type' parameter must be a type of '{{nameof(NotificationType)}}'. Instead, got '{type}'.");
            
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (Database.NotificationCenter.GetStored(out var storedNotifications, postponed))
            {
                writer.WriteStartObject();
                
                var countQuery = pageSize == 0;
                var totalResults = 0;
                var isFirst = true;
                
                writer.WritePropertyName("Results");
                writer.WriteStartArray();
                foreach (var notification in storedNotifications)
                {
                    if (shouldFilter && notification.Json != null)
                    {
                        if (notification.Json.TryGet(nameof(Notification.Type), out string notificationType) == false)
                            continue;
                        
                        if (notificationType != type)
                            continue;
                    }
                    
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }

                    totalResults++;
                    
                    if (pageSize == 0 && countQuery == false)
                        break;
                    pageSize--;
                    
                    if (countQuery)
                        continue;
                    
                    if (isFirst == false)
                    {
                        writer.WriteComma();
                    }
                    
                    writer.WriteObject(notification.Json);
                    isFirst = false;
                }
                writer.WriteEndArray();
                
                writer.WriteComma();
                writer.WritePropertyName("TotalResults");
                writer.WriteInteger(totalResults);
                
                writer.WriteEndObject();
            }
        }
        
        [RavenAction("/databases/*/notification-center/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Get()
        {
            try
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
            catch (OperationCanceledException)
            {
                // disposing
            }
            catch (ObjectDisposedException)
            {
                // disposing
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
