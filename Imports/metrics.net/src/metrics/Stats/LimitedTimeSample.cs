using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace metrics.Stats
{
    /// <summary>
    /// A sample of items from the recent past
    /// </summary>
    public class LimitedTimeSample : ISample<LimitedTimeSample>
    {
        private readonly IDateTimeSupplier _dateTimeSupplier = new DateTimeSupplier();
        private readonly TimeSpan _timeToKeepItems;
        private readonly TimeSpan _timeBetweenRemovingOldItems;
        private ConcurrentBag<Item> _items = new ConcurrentBag<Item>();
        private DateTime _timeToNextRebuildIndex;
        private readonly object _rebuildLock = new object();

        /// <summary>
        /// for testing
        /// </summary>
        public LimitedTimeSample(IDateTimeSupplier dateTimeSupplier, TimeSpan timeToKeepItems, TimeSpan timeBetweenRemovingOldItems)
            : this(timeToKeepItems, timeBetweenRemovingOldItems)
        {
            _dateTimeSupplier = dateTimeSupplier;
            _timeToNextRebuildIndex = _dateTimeSupplier.UtcNow.Add(timeBetweenRemovingOldItems);
        }

        public LimitedTimeSample(TimeSpan timeToKeepItems, TimeSpan timeBetweenRemovingOldItems)
        {
            _timeToNextRebuildIndex = _dateTimeSupplier.UtcNow.Add(timeBetweenRemovingOldItems);
            _timeToKeepItems = timeToKeepItems;
            _timeBetweenRemovingOldItems = timeBetweenRemovingOldItems;
        }

        private LimitedTimeSample(ConcurrentBag<Item> items)
        {
            _items = items;
        }

        private struct Item
        {
            internal DateTime DateTimeArrived { get; set; }
            internal long Value { get; set; }
        }

        public void Clear()
        {
            _items = new ConcurrentBag<Item>();
        }

        public int Count
        {
            get { return _items.Count; }
        }

        public void Update(long value)
        {
            var now = _dateTimeSupplier.UtcNow;
            _items.Add(new Item { DateTimeArrived = now, Value = value });
            if (now < _timeToNextRebuildIndex) 
                return;
            lock (_rebuildLock)
            {
                if (now < _timeToNextRebuildIndex) 
                    return;
                RebuildIndex(now);
                _timeToNextRebuildIndex = now.Add(_timeBetweenRemovingOldItems);
            }
        }

        private void RebuildIndex(DateTime now)
        {
            var oldestPossibleItem = now.Subtract(_timeToKeepItems);
            var recentItems = _items.Where(x => x.DateTimeArrived > oldestPossibleItem);
            _items = new ConcurrentBag<Item>(recentItems);
        }

        public ICollection<long> Values
        {
            get { return _items.Select(x => x.Value).ToArray(); }
        }

        public LimitedTimeSample Copy
        {
            get { return new LimitedTimeSample(_items); }
        }
    }
}