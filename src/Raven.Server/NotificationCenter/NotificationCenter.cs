using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter : IDisposable
    {
        private static readonly string PagingDocumentsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Documents}";
        private static readonly string PagingQueriesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Queries}";
        private static readonly string PagingRevisionsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Revisions}";

        private readonly ConcurrentSet<ConnectedWatcher> _watchers = new ConcurrentSet<ConnectedWatcher>();
        private readonly List<BackgroundWorkBase> _backgroundWorkers = new List<BackgroundWorkBase>();
        private readonly NotificationsStorage _notificationsStorage;
        private readonly object _watchersLock = new object();
        private readonly string _resourceName;
        private readonly CancellationToken _shutdown;
        private PostponedNotificationsSender _postponedNotifications;

        private readonly ConcurrentQueue<(PagingOperationType, string, int, int, DateTime)> _pagingQueue = new ConcurrentQueue<(PagingOperationType, string, int, int, DateTime)>();
        private readonly DateTime[] _pagingUpdates = new DateTime[Enum.GetNames(typeof(PagingOperationType)).Length];
        private Timer _pagingTimer;

        public NotificationCenter(NotificationsStorage notificationsStorage, string resourceName, CancellationToken shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _resourceName = resourceName;
            _shutdown = shutdown;
        }

        public void Initialize(DocumentDatabase database = null)
        {
            _postponedNotifications = new PostponedNotificationsSender(_resourceName, _notificationsStorage,
                    _watchers, _shutdown);

            _backgroundWorkers.Add(_postponedNotifications);

            if (database != null)
            {
                _backgroundWorkers.Add(new DatabaseStatsSender(database, this));
                _pagingTimer = new Timer(UpdatePaging, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
        }

        public NotificationCenterOptions Options { get; } = new NotificationCenterOptions();

        private void StartBackgroundWorkers()
        {
            foreach (var worker in _backgroundWorkers)
            {
                worker.Start();
            }
        }

        private void StopBackgroundWorkers()
        {
            foreach (var worker in _backgroundWorkers)
            {
                worker.Stop();
            }
        }

        public IDisposable TrackActions(AsyncQueue<Notification> notificationsQueue, IWebsocketWriter webSockerWriter)
        {
            var watcher = new ConnectedWatcher
            {
                NotificationsQueue = notificationsQueue,
                Writer = webSockerWriter
            };

            lock (_watchersLock)
            {
                _watchers.TryAdd(watcher);

                if (_watchers.Count == 1)
                    StartBackgroundWorkers();
            }

            return new DisposableAction(() =>
            {
                lock (_watchersLock)
                {
                    _watchers.TryRemove(watcher);

                    if (_watchers.Count == 0)
                        StopBackgroundWorkers();
                }
            });
        }

        public void AddPaging(PagingOperationType operation, string action, int numberOfResults, int pageSize)
        {
            var now = SystemTime.UtcNow;
            var update = _pagingUpdates[(int)operation];

            if (now - update < TimeSpan.FromSeconds(15))
                return;

            _pagingUpdates[(int)operation] = now;
            _pagingQueue.Enqueue((operation, action, numberOfResults, pageSize, now));

            while (_pagingQueue.Count > 50)
                _pagingQueue.TryDequeue(out _);
        }

        public void Add(Notification notification)
        {
            if (notification.IsPersistent)
            {
                if (_notificationsStorage.Store(notification) == false)
                    return;
            }

            if (_watchers.Count == 0)
                return;

            NotificationTableValue existing;
            using (_notificationsStorage.Read(notification.Id, out existing))
            {
                if (existing?.PostponedUntil > SystemTime.UtcNow)
                    return;
            }

            // ReSharper disable once InconsistentlySynchronizedField
            foreach (var watcher in _watchers)
            {
                watcher.NotificationsQueue.Enqueue(notification);
            }
        }

        public void AddAfterTransactionCommit(Notification notification, RavenTransaction tx)
        {
            var llt = tx.InnerTransaction.LowLevelTransaction;

            llt.OnDispose += _ =>
            {
                if (llt.Committed == false)
                    return;

                Add(notification);
            };
        }

        public IDisposable GetStored(out IEnumerable<NotificationTableValue> actions, bool postponed = true)
        {
            var scope = _notificationsStorage.ReadActionsOrderedByCreationDate(out actions);

            if (postponed)
                return scope;

            var now = SystemTime.UtcNow;

            actions = actions.Where(x => x.PostponedUntil == null || x.PostponedUntil <= now);

            return scope;
        }

        public long GetAlertCount()
        {
            return _notificationsStorage.GetAlertCount();
        }

        public void Dismiss(string id)
        {
            var deleted = _notificationsStorage.Delete(id);

            if (deleted == false)
                return;

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Dismissed));
        }

        public void Postpone(string id, DateTime until)
        {
            _notificationsStorage.ChangePostponeDate(id, until);

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Postponed));

            _postponedNotifications?.Set();
        }

        public void Dispose()
        {
            _pagingTimer?.Dispose();

            foreach (var worker in _backgroundWorkers)
            {
                worker.Dispose();
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
                Add(documents);

            if (queries != null)
                Add(queries);

            if (revisions != null)
                Add(revisions);
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
                        return PerformanceHint.Create("Page size too big (documents)", "We have detected that some of the requests are returning excessive amount of documents. Consider using smaller page sizes or streaming operations.", PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                    case PagingOperationType.Queries:
                        return PerformanceHint.Create("Page size too big (queries)", "We have detected that some of the requests are returning excessive amount of query results. Consider using smaller page sizes or streaming operations.", PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                    case PagingOperationType.Revisions:
                        return PerformanceHint.Create("Page size too big (revisions)", "We have detected that some of the requests are returning excessive amount of revisions. Consider using smaller page sizes.", PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                }
            }
        }

        public class ConnectedWatcher
        {
            public AsyncQueue<Notification> NotificationsQueue;

            public IWebsocketWriter Writer;
        }
    }

    internal class PagingPerformanceDetails : INotificationDetails
    {
        public PagingPerformanceDetails()
        {
            Actions = new Dictionary<string, Queue<ActionDetails>>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, Queue<ActionDetails>> Actions { get; set; }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            foreach (var key in Actions.Keys)
            {
                var queue = Actions[key];
                if (queue == null)
                    continue;

                var list = new DynamicJsonArray();
                foreach (var details in queue)
                {
                    list.Add(new DynamicJsonValue
                    {
                        [nameof(ActionDetails.NumberOfResults)] = details.NumberOfResults,
                        [nameof(ActionDetails.PageSize)] = details.PageSize,
                        [nameof(ActionDetails.Occurence)] = details.Occurence
                    });
                }

                djv[key] = list;
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Actions)] = djv
            };
        }

        public void Update(string action, int numberOfResults, int pageSize, DateTime occurence)
        {
            Queue<ActionDetails> details;
            if (Actions.TryGetValue(action, out details) == false)
                Actions[action] = details = new Queue<ActionDetails>();

            details.Enqueue(new ActionDetails { Occurence = occurence, NumberOfResults = numberOfResults, PageSize = pageSize });

            while (details.Count > 10)
                details.Dequeue();
        }

        internal class ActionDetails
        {
            public DateTime Occurence { get; set; }
            public int NumberOfResults { get; set; }
            public int PageSize { get; set; }
        }
    }

    public enum PagingOperationType
    {
        Documents,
        Queries,
        Revisions
    }
}