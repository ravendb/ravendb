using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesRangeResult : IPostJsonDeserialization
    {
        public DateTime From, To;

        // Backing field for server-returned entries
        private TimeSeriesEntry[] _entries;

        // Public API stays EXACTLY the same
        public TimeSeriesEntry[] Entries
        {
            get => MergeWithCached(_entries, CachedEntries);
            set
            {
                _entries = MergeWithCached(value, CachedEntries);
                CachedEntries.Clear();
                if (_entries != null)
                    for (int i = 0; i < _entries.Length; i++)
                        CachedEntries.Add(_entries[i]);
            }
        }

        // Your new cached list — always base type
        internal List<TimeSeriesEntry> CachedEntries { get; set; } = new();

        public long? TotalResults;
        internal string Hash;

        [JsonIgnore] internal bool IsLocal { get; set; }
        [JsonIgnore] internal bool IsDeleted { get; set; }

        public BlittableJsonReaderObject Includes;
        internal List<string> MissingIncludes;

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context) => SetMinMaxDateTime();

        void IPostJsonDeserialization.PostDeserialization() => SetMinMaxDateTime();

        public TimeSeriesRangeResult CloneRange(DateTime from, DateTime to)
        {
            var clone = new TimeSeriesRangeResult
            {
                From = from,
                IsLocal = this.IsLocal,
                To = to,
                CachedEntries = new List<TimeSeriesEntry>()
            };

            // Copy only entries inside [from, to)
            for (int i = 0; i < this.CachedEntries.Count; i++)
            {
                var entry = (TimeSeriesEntry)this.CachedEntries[i];
                var ts = entry.Timestamp;

                if (ts >= from && ts <= to)
                {
                    clone.CachedEntries.Add(entry);
                }
            }

            return clone;
        }

        protected static TEntry[] MergeWithCached<TEntry>(
            TEntry[] serverEntries,
            List<TimeSeriesEntry> cached)
            where TEntry : TimeSeriesEntry
        {
            serverEntries ??= Array.Empty<TEntry>();
            cached ??= new List<TimeSeriesEntry>();

            int serverCount = serverEntries.Length;
            int cachedCount = cached.Count;

            if (serverCount == 0 && cachedCount == 0)
                return Array.Empty<TEntry>();

            // Worst-case size: serverCount + cachedCount
            // Dictionary ensures uniqueness by timestamp
            var map = new Dictionary<DateTime, TEntry>(serverCount + cachedCount);

            // Insert server entries first
            for (int i = 0; i < serverCount; i++)
            {
                var s = serverEntries[i];
                map[s.Timestamp] = s;
            }

            // Insert cached entries (override server on conflict)
            for (int i = 0; i < cachedCount; i++)
            {
                if (cached[i] is TEntry c)
                    map[c.Timestamp] = c;
            }

            // Convert dictionary values to array
            int count = map.Count;
            var result = new TEntry[count];
            map.Values.CopyTo(result, 0);

            // Sort by timestamp
            Array.Sort(result, (a, b) => a.Timestamp.CompareTo(b.Timestamp));

            return result;
        }


        private void SetMinMaxDateTime()
        {
            if (From == default)
                From = DateTime.MinValue;
            if (To == default)
                To = DateTime.MaxValue;
        }
    }


    public sealed class TimeSeriesRangeResult<TValues> : TimeSeriesRangeResult
        where TValues : TimeSeriesEntry
    {
        private TValues[] _entries;
        public new TValues[] Entries
        {
            get => MergeWithCached(_entries, CachedEntries);
            set
            {
                _entries = MergeWithCached(value, CachedEntries);
                CachedEntries.Clear();
                if (_entries != null)
                    for (int i = 0; i < _entries.Length; i++)
                        CachedEntries.Add(_entries[i]);
            }
        }
    }

}
