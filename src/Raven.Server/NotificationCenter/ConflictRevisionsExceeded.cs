using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;

using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Logging;
using Sparrow.Json;
using System.Collections;
using Sparrow.Server.Utils;

namespace Raven.Server.NotificationCenter;
public class ConflictRevisionsExceeded
{
    private static readonly string ConflictRevisionExceededMaxId = "ConflictRevisionExceededMax";

    private readonly NotificationCenter _notificationCenter;
    private readonly NotificationsStorage _notificationsStorage;
    private readonly string _database;

    private readonly object _locker = new object();
    public static readonly long QueueMaxSize = 64;
    private readonly ConcurrentQueue<ConflictInfo> _queue = new ConcurrentQueue<ConflictInfo>();
    public enum ExceedingReason
    {
        MinimumRevisionAgeToKeep,
        MinimumRevisionsToKeep
    }

    private Timer _timer;
    private readonly Logger _logger;

    public ConflictRevisionsExceeded(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
    {
        _notificationCenter = notificationCenter;
        _notificationsStorage = notificationsStorage;
        _database = database;
        _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
    }

    public void Add(ConflictInfo info)
    {
        _queue.Enqueue(info);

        while (_queue.Count > QueueMaxSize)
            _queue.TryDequeue(out _);

        if (_timer != null)
            return;

        lock (_locker)
        {
            if (_timer != null)
                return;

            _timer = new Timer(Update, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }
    }

    private void Update(object state)
    {
        try
        {
            if (_queue.IsEmpty)
                return;

            var alert = GetConflictRevisionsPerformanceAlert();
            var details = ((ConflictPerformanceDetails)alert.Details);
            while (_queue.TryDequeue(out ConflictInfo pagingInfo))
            {
                details.Update(pagingInfo);
            }

            _notificationCenter.Add(alert);
        }
        catch (Exception e)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info("Error in a notification center revisions timer", e);
        }
    }


    private AlertRaised GetConflictRevisionsPerformanceAlert()
    {
        using (_notificationsStorage.Read(ConflictRevisionExceededMaxId, out NotificationTableValue ntv))
        {
            using (ntv)
            {
                ConflictPerformanceDetails details;
                if (ntv == null || ntv.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    details = new ConflictPerformanceDetails();
                else
                    details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<ConflictPerformanceDetails>(detailsJson, ConflictRevisionExceededMaxId);

                return AlertRaised.Create(_database, "Excess number of Conflict Revisions",
                    "We have detected that some of the documents conflict/resolved revisions exceeded the configured value (set on the conflict revisions configuration).",
                    AlertType.ConflictRevisionsExceeded, NotificationSeverity.Warning, ConflictRevisionExceededMaxId, details);
            }
        }
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }


    public readonly struct ConflictInfo
    {
        public string IdPrefix { get; }

        public ExceedingReason Reason { get; }
        public long Deleted { get; }
        public DateTime Time { get; }

        public ConflictInfo(string idPrefix, ExceedingReason reason, long numberOfDeletedConficts, DateTime time)
        {
            IdPrefix = idPrefix;
            Reason = reason;
            Deleted = numberOfDeletedConficts;
            Time = time;
        }

        public string GetId()
        {
            if (IdPrefix.EndsWith((char)SpecialChars.RecordSeparator))
                return IdPrefix.Substring(0, IdPrefix.Length - 1); // cut the prefix last char (SpecialChars.RecordSeparator)
            return IdPrefix;
        }
    }

}


