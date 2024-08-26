using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public sealed class Paging : IDisposable
    {
        private static readonly string PagingDocumentsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Documents}";
        private static readonly string PagingQueriesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Queries}";
        private static readonly string PagingRevisionsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.Revisions}";
        private static readonly string PagingCompareExchangeId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.Paging}/{PagingOperationType.CompareExchange}";

        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        private readonly object _locker = new object();
        private readonly ConcurrentQueue<PagingInformation> _pagingQueue = new();
        private readonly DateTime[] _pagingUpdates = new DateTime[Enum.GetNames(typeof(PagingOperationType)).Length];
        private Timer _pagingTimer;
        private readonly RavenLogger _logger;

        public Paging([NotNull] AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter ?? throw new ArgumentNullException(nameof(notificationCenter));

            _logger = RavenLogManager.Instance.GetLoggerForDatabase<Paging>(notificationCenter.Database);
        }

        public void Add(PagingOperationType operation, string action, string details, long numberOfResults, long pageSize, long duration, long totalDocumentsSizeInBytes)
        {
            var now = SystemTime.UtcNow;
            var update = _pagingUpdates[(int)operation];

            if (now - update < TimeSpan.FromSeconds(15))
                return;

            _pagingUpdates[(int)operation] = now;
            _pagingQueue.Enqueue(new PagingInformation(operation, action, details, numberOfResults, pageSize, duration, now, totalDocumentsSizeInBytes));

            if (ForTestingPurposes?.DisableDequeue == true)
                return;

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

        private void UpdatePaging(object state)
        {
            UpdatePagingInternal(state, out _);
        }

        internal bool UpdatePagingInternal(object state, out string reasonOfNotUpdating)
        {
            var outcome = false;
            reasonOfNotUpdating = "";

            try
            {
                if (_pagingQueue.IsEmpty)
                {
                    reasonOfNotUpdating += "Queue is empty";
                    return false;
                }

                PerformanceHint documents = null, queries = null, revisions = null, compareExchange = null;
                while (_pagingQueue.TryDequeue(
                    out PagingInformation pagingInfo))
                {
                    switch (pagingInfo.Type)
                    {
                        case PagingOperationType.Documents:
                            documents ??= GetPagingPerformanceHint(PagingDocumentsId, pagingInfo.Type);
                            ((PagingPerformanceDetails)documents.Details).Update(pagingInfo);
                            break;

                        case PagingOperationType.Queries:
                            queries ??= GetPagingPerformanceHint(PagingQueriesId, pagingInfo.Type);
                            ((PagingPerformanceDetails)queries.Details).Update(pagingInfo);
                            break;

                        case PagingOperationType.Revisions:
                            revisions ??= GetPagingPerformanceHint(PagingRevisionsId, pagingInfo.Type);
                            ((PagingPerformanceDetails)revisions.Details).Update(pagingInfo);
                            break;

                        case PagingOperationType.CompareExchange:
                            compareExchange ??= GetPagingPerformanceHint(PagingCompareExchangeId, pagingInfo.Type);
                            ((PagingPerformanceDetails)compareExchange.Details).Update(pagingInfo);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                if (documents != null)
                {
                    _notificationCenter.Add(documents);
                    outcome = true;
                }

                if (queries != null)
                {
                    _notificationCenter.Add(queries);
                    outcome = true;
                }

                if (revisions != null)
                {
                    _notificationCenter.Add(revisions);
                    outcome = true;
                }

                if (compareExchange != null)
                {
                    _notificationCenter.Add(compareExchange);
                    outcome = true;
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in a notification center paging timer", e);

                outcome = false;
                reasonOfNotUpdating += $"Error in a notification center paging timer. {e}";
            }

            return outcome;
        }

        private PerformanceHint GetPagingPerformanceHint(string id, PagingOperationType type)
        {
            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    PagingPerformanceDetails details;
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                        details = new PagingPerformanceDetails();
                    else
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<PagingPerformanceDetails>(detailsJson, id);

                    switch (type)
                    {
                        case PagingOperationType.Documents:
                        case PagingOperationType.Queries:
                            return PerformanceHint.Create(_notificationCenter.Database, $"Page size too big ({type.ToString().ToLower()})",
                                "We have detected that some of the requests are returning excessive amount of documents. Consider using smaller page sizes or streaming operations.",
                                PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                        case PagingOperationType.Revisions:
                            return PerformanceHint.Create(_notificationCenter.Database, "Page size too big (revisions)",
                                "We have detected that some of the requests are returning excessive amount of revisions. Consider using smaller page sizes.",
                                PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                        case PagingOperationType.CompareExchange:
                            return PerformanceHint.Create(_notificationCenter.Database, "Page size too big (compare exchange)",
                                "We have detected that some of the requests are returning excessive amount of compare exchange values. Consider using smaller page sizes.",
                                PerformanceHintType.Paging, NotificationSeverity.Warning, type.ToString(), details);
                        default:
                            throw new ArgumentOutOfRangeException(nameof(type), type, null);
                    }
                }
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal bool DisableDequeue;
        }

        public void Dispose()
        {
            _pagingTimer?.Dispose();
        }

        internal readonly struct PagingInformation
        {
            public PagingOperationType Type { get; }
            public string Action { get; }
            public string Details { get; }
            public long NumberOfResults { get; }
            public long PageSize { get; }
            public long Duration { get; }
            public DateTime Occurrence { get; }
            public long TotalDocumentsSizeInBytes { get; }

            public PagingInformation(PagingOperationType type, string action, string details, long numberOfResults, long pageSize, long duration, DateTime occurrence, long totalDocumentsSizeInBytes)
            {
                Type = type;
                Action = action;
                Details = details;
                NumberOfResults = numberOfResults;
                PageSize = pageSize;
                Duration = duration;
                Occurrence = occurrence;
                TotalDocumentsSizeInBytes = totalDocumentsSizeInBytes;
            }
        }
    }
}
