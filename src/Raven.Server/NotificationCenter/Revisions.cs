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

namespace Raven.Server.NotificationCenter;
public class Revisions
{
    private static readonly string ConflictRevisionExceededMaxId = "ConflictRevisionExceededMax";

    private readonly NotificationCenter _notificationCenter;
    private readonly NotificationsStorage _notificationsStorage;
    private readonly string _database;

    private readonly object _locker = new object();
    private readonly DetailsUniqueQueue _queue = new DetailsUniqueQueue(maxSize: 64);
    public enum ExceedingReason
    {
        MinimumRevisionAgeToKeep,
        MinimumRevisionsToKeep
    }

    private Timer _timer;
    private readonly Logger _logger;

    public Revisions(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
    {
        _notificationCenter = notificationCenter;
        _notificationsStorage = notificationsStorage;
        _database = database;
        _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
    }

    public void Add(ConflictInfo info)
    {
        _queue.Enqueue(info);

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

            PerformanceHint hint = GetPagingPerformanceHint();
            var details = ((ConflictPerformanceDetails)hint.Details);
            while (_queue.TryDequeue(out ConflictInfo pagingInfo))
            {
                details.Update(pagingInfo);
            }

            _notificationCenter.Add(hint);
        }
        catch (Exception e)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info("Error in a notification center revisions timer", e);
        }
    }


    private PerformanceHint GetPagingPerformanceHint()
    {
        using (_notificationsStorage.Read(ConflictRevisionExceededMaxId, out NotificationTableValue ntv))
        {
            ConflictPerformanceDetails details;
            if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                details = new ConflictPerformanceDetails();
            else
                details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<ConflictPerformanceDetails>(detailsJson, ConflictRevisionExceededMaxId);

            return PerformanceHint.Create(_database, "Excess number of Conflict Revisions",
                "We have detected that some of the documents conflict/resolved revisions exceeded the configured value (set on the conflict revisions configuration).",
                PerformanceHintType.Revisions, NotificationSeverity.Warning, ConflictRevisionExceededMaxId, details);
        }
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }


    public readonly struct ConflictInfo
    {
        public string Id { get; }
        public ExceedingReason Reason { get; }
        public long Deleted { get; }
        public DateTime Time { get; }

        public ConflictInfo(string id, ExceedingReason reason, long deleted, DateTime time)
        {
            Id = id;
            Reason = reason;
            Deleted = deleted;
            Time = time;
        }
    }

    private class DetailsUniqueQueue : ConcurrentUniqueQueue<string, ConflictInfo>
    {
        /*
         * Queue that doesnt have duplicatons.
         * If you enqueue info about the same doc twice, then the old info
         * will be removed (from the middle of the node), and will be added to the last of the node.
         * This will happen efficiently (O(1)).
         */

        public DetailsUniqueQueue(long maxSize) : base(maxSize)
        {
        }

        public void Enqueue(ConflictInfo info)
        {
            base.Enqueue(info.Id, info);
        }

        public bool TryDequeue(out ConflictInfo info)
        {
            return base.TryDequeue(out _, out info);
        }
    }

    private class ConcurrentUniqueQueue<T, E> : IEnumerable<E>
        where T : class
    {
        private readonly ConcurrentDictionary<T, NodeHolder> _dictionary;
        private readonly LinkedList<T> _linkedList;
        private readonly long _maxSize;
        public int Count => _linkedList.Count;
        public bool IsEmpty => _linkedList.Count == 0;


        public ConcurrentUniqueQueue(long maxSize)
        {
            _dictionary = new ConcurrentDictionary<T, NodeHolder>();
            _linkedList = new LinkedList<T>();
            _maxSize = maxSize;
        }

        public void Enqueue(T key, E value)
        {
            if (_dictionary.TryGetValue(key, out var h))
            {
                // If the key already exists, remove it from its current position and re-enqueue at the end.
                _linkedList.Remove(h.ExistingNode); // O(1) - LinkedListNode has references also to the previous node
                _linkedList.AddLast(h.ExistingNode);
                h.Value = value;
            }
            else
            {
                // If the key doesn't exist, create a new node and add it to the _dictionary and the end of the list.
                var newNode = new LinkedListNode<T>(key);
                _linkedList.AddLast(newNode);
                var h1 = new NodeHolder() { ExistingNode = newNode, Value = value };
                _dictionary.TryAdd(key, h1);
            }

            if (_maxSize < Count)
            {
                TryDequeue(out var k, out var _);
            }
        }

        public bool TryDequeue(out T key, out E value)
        {
            key = default;
            value = default;
            if (_linkedList.First != null)
            {
                var firstNode = _linkedList.First;
                _linkedList.RemoveFirst();
                _dictionary.TryRemove(firstNode.Value, out var h);
                key = firstNode.Value;
                value = h.Value;
                return true;
            }

            return false;
        }

        public IEnumerator<E> GetEnumerator()
        {
            foreach (var key in _linkedList)
            {
                yield return _dictionary[key].Value;
            }
        }

        public List<E> GetList()
        {
            return _linkedList.Select(key => _dictionary[key].Value).ToList();
        }

        public void Clear()
        {
            _dictionary.Clear();
            _linkedList.Clear();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private class NodeHolder
        {
            public LinkedListNode<T> ExistingNode;
            public E Value;
        }

    }

}


