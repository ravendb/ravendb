using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesRangeResult : IPostJsonDeserialization
    {
        public DateTime From, To;
        public TimeSeriesEntry[] Entries;
        public long? TotalResults;
        internal string Hash;

        public BlittableJsonReaderObject Includes;
        internal List<string> MissingIncludes;

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            SetMinMaxDateTime();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            SetMinMaxDateTime();
        }
        
        private void SetMinMaxDateTime()
        {
            if (From == default)
                From = DateTime.MinValue;
            if (To == default)
                To = DateTime.MaxValue;
        }
    }

    public class TimeSeriesRangeResult<TValues> : TimeSeriesRangeResult where TValues : TimeSeriesEntry
    {
        public new TValues[] Entries;
    }
}
