using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesRangeResult : IPostJsonDeserialization
    {
        public DateTime From, To;
        private TimeSeriesEntry[] _entries;

        public TimeSeriesEntry[] Entries
        {
            get => CachedEntries.ToArray();
            set
            {
                _entries = MergeWithCached(value, CachedEntries);
                CachedEntries.Clear();
                if (_entries != null)
                    for (int i = 0; i < _entries.Length; i++)
                        CachedEntries.Add(_entries[i]);
            }
        }

        internal List<TimeSeriesEntry> CachedEntries { get; set; } = new();

        public long? TotalResults;
        internal string Hash;

        [JsonIgnore] internal bool IsDeleted { get; set; }

        public BlittableJsonReaderObject Includes;
        internal List<string> MissingIncludes;

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context) => SetMinMaxDateTime();

        void IPostJsonDeserialization.PostDeserialization() => SetMinMaxDateTime();

        protected static TEntry[] MergeWithCached<TEntry>(TEntry[] serverEntries, List<TimeSeriesEntry> cached)
            where TEntry : TimeSeriesEntry
        {
            serverEntries ??= Array.Empty<TEntry>();
            int serverCount = serverEntries.Length;
            int cachedCount = cached?.Count ?? 0;

            if (serverCount == 0 && cachedCount == 0)
                return Array.Empty<TEntry>();

            var result = new List<TEntry>(serverCount + cachedCount);

            int i = 0, j = 0;
            while (i < serverCount && j < cachedCount)
            {
                if (!(cached[j] is TEntry c))
                {
                    j++;
                    continue;
                }

                var s = serverEntries[i];
                int cmp = s.Timestamp.CompareTo(c.Timestamp);
                if (cmp < 0)
                {
                    result.Add(s);
                    i++;
                }
                else if (cmp > 0)
                {
                    result.Add(c);
                    j++;
                }
                else
                {
                    // same timestamp -> cached wins
                    result.Add(c);
                    i++;
                    j++;
                }
            }

            while (i < serverCount)
                result.Add(serverEntries[i++]);

            while (j < cachedCount)
            {
                if (cached[j] is TEntry c)
                    result.Add(c);
                j++;
            }

            return result.ToArray();
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
            get => CachedEntries.OfType<TValues>().ToArray();
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
