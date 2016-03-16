using System;

using Raven.Client.Data.Indexes;
using  Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    public class IndexingPerformanceStats
    {
        public string Operation { get; set; }
        public int ItemsCount { get; set; }
        public int InputCount { get; set; }
        public int OutputCount { get; set; }
        public DateTime Started { get; set; }
        public DateTime Completed { get; set; }
        public TimeSpan Duration { get; set; }
        public double DurationMilliseconds { get { return Math.Round(Duration.TotalMilliseconds, 2); } }
        [JsonProperty(ItemTypeNameHandling = TypeNameHandling.Objects)]

        public BasePerformanceStats[] Operations { get; set; }

        public TimeSpan WaitingTimeSinceLastBatchCompleted { get; set; }
        [JsonIgnore]
        public Action OnCompleted = delegate { };

        public void RunCompleted()
        {
            var onCompleted = OnCompleted;
            OnCompleted = null;
            if (onCompleted != null)
                onCompleted();
        }
    }
}
