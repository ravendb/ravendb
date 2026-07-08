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

        public TimeSeriesEntry[] Entries
        {
            get => CachedEntries.ToArray();
            set
            {
                CachedEntries.Clear();
                if (value != null)
                    CachedEntries.AddRange(value);
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
        public new TValues[] Entries
        {
            get => CachedEntries.OfType<TValues>().ToArray();
            set
            {
                CachedEntries.Clear();
                if (value != null)
                    for (int i = 0; i < value.Length; i++)
                        CachedEntries.Add(value[i]);
            }
        }
    }

}
