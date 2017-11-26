using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class RequestLatency
    {
        private static readonly string RequestLatenciesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.RequestLatency}";
        private readonly object _addHintSyncObj = new object();
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        
        public RequestLatency(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
        }

        public void AddHint(string queryString, IQueryCollection requestQuery, long duration, string action)
        {
            lock (_addHintSyncObj)
            {
                var requestLatencyPerformanceHint = GetOrCreatePerformanceLatencies(out var details);
                details.Update(queryString, requestQuery, duration, action);
                _notificationCenter.Add(requestLatencyPerformanceHint);
            }
        }

        public RequestLatencyDetail GetRequestLatencyDetails()
        {
            GetOrCreatePerformanceLatencies(out var details);
            return details;
        }

        private PerformanceHint GetOrCreatePerformanceLatencies(out RequestLatencyDetail details)
        {
            //Read() is transactional, so this is thread-safe
            using (_notificationsStorage.Read(RequestLatenciesId, out var ntv))
            {
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                {
                    details = new RequestLatencyDetail();
                }
                else
                {
                    details = (RequestLatencyDetail)EntityToBlittable.ConvertToEntity(
                        typeof(RequestLatencyDetail),
                        RequestLatenciesId,
                        detailsJson,
                        DocumentConventions.Default);
                }

                return PerformanceHint.Create(
                    "Request latency is too high",
                    "We have detected that some query duration has surpassed the configured threshold",
                    PerformanceHintType.RequestLatency,
                    NotificationSeverity.Warning,
                    "Query Latency",
                    details
                );
            }
        }
    }
}
