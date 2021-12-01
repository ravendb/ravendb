using System;
using System.Linq;
using System.Runtime.Serialization;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesRangeResult : IPostJsonDeserialization
    {
        public DateTime From, To;
        public TimeSeriesEntry[] Entries;
        public long? TotalResults;
        internal string Hash;

        public BlittableJsonReaderObject Includes;

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
