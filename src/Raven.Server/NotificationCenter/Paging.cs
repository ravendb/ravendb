using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class Paging : IDisposable
    {
        private static readonly string PagingDocumentsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Documents}";
        private static readonly string PagingQueriesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Queries}";
        private static readonly string PagingRevisionsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Revisions}";

        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;

        private readonly object _locker = new object();
        private readonly ConcurrentQueue<(PagingOperationType, string, int, int, DateTime)> _pagingQueue = new ConcurrentQueue<(PagingOperationType, string, int, int, DateTime)>();
        private readonly DateTime[] _pagingUpdates = new DateTime[Enum.GetNames(typeof(PagingOperationType)).Length];
        private Timer _pagingTimer;

        public Paging(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
        }

        public void Add(PagingOperationType operation, string action, int numberOfResults, int pageSize)
        {
            var now = SystemTime.UtcNow;
            var update = _pagingUpdates[(int)operation];

            if (now - update < TimeSpan.FromSeconds(15))
                return;

            _pagingUpdates[(int)operation] = now;
            _pagingQueue.Enqueue((operation, action, numberOfResults, pageSize, now));

            while (_pagingQueue.Count > 50)
                _pagingQueue.TryDequeue(out _);

            if (_pagingTimer != null)
                return;

            lock (_locker)
            {
                if (_pagingTimer != null)
                    return;

                _pagingTimer = new Timer(UpdatePaging, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
        }

        internal void UpdatePaging(object state)
        {
            if (_pagingQueue.IsEmpty)
                return;

            PerformanceHint documents = null, queries = null, revisions = null;

            (PagingOperationType, string, int, int, DateTime) tuple;
            while (_pagingQueue.TryDequeue(out tuple))
            {
                switch (tuple.Item1)
                {
                    case PagingOperationType.Documents:
                        if (documents == null)
                            documents = GetPagingPerformanceHint(PagingDocumentsId, tuple.Item1);

                        ((PagingPerformanceDetails)documents.Details).Update(tuple.Item2, tuple.Item3, tuple.Item4, tuple.Item5);

                        break;
                    case PagingOperationType.Queries:
                        if (queries == null)
                            queries = GetPagingPerformanceHint(PagingQueriesId, tuple.Item1);

                        ((PagingPerformanceDetails)queries.Details).Update(tuple.Item2, tuple.Item3, tuple.Item4, tuple.Item5);

                        break;
                    case PagingOperationType.Revisions:
                        if (revisions == null)
                            revisions = GetPagingPerformanceHint(PagingRevisionsId, tuple.Item1);

                        ((PagingPerformanceDetails)revisions.Details).Update(tuple.Item2, tuple.Item3, tuple.Item4, tuple.Item5);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if (documents != null)
                _notificationCenter.Add(documents);

            if (queries != null)
                _notificationCenter.Add(queries);

            if (revisions != null)
                _notificationCenter.Add(revisions);
        }

        private PerformanceHint GetPagingPerformanceHint(string id, PagingOperationType type)
        {
            NotificationTableValue ntv;
            using (_notificationsStorage.Read(id, out ntv))
            {
                PagingPerformanceDetails details;
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    details = new PagingPerformanceDetails();
                else
                    details = (PagingPerformanceDetails)EntityToBlittable.ConvertToEntity(typeof(PagingPerformanceDetails), id, detailsJson, DocumentConventions.Default);

                switch (type)
                {
                    case PagingOperationType.Documents:
                    case PagingOperationType.Queries:
                        return PerformanceHint.Create($"Page size too big ({type.ToString().ToLower()})", "We have detected that some of the requests are returning excessive amount of documents. Consider using smaller page sizes or streaming operations.", PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                    case PagingOperationType.Revisions:
                        return PerformanceHint.Create("Page size too big (revisions)", "We have detected that some of the requests are returning excessive amount of revisions. Consider using smaller page sizes.", PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                }
            }
        }

        public void Dispose()
        {
            _pagingTimer?.Dispose();
        }
    }
}