// -----------------------------------------------------------------------
//  <copyright file="ReducingBatchInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using Raven.Client.Data.Indexes;

namespace Raven.Abstractions.Data
{
    public class ReducingBatchInfo
    {
        public long Id { get; set; }

        public List<string> IndexesToWorkOn { get; set; }

        public DateTime StartedAt { get; set; }

        public double TotalDurationMs { get; set; }

        public double TimeSinceFirstReduceInBatchCompletedMs { get; set; }

        public ConcurrentDictionary<string, ReducingPerformanceStats[]> PerformanceStats { get; set; }

        public void BatchCompleted()
        {
            var now = SystemTime.UtcNow;
            TotalDurationMs = (now - StartedAt).TotalMilliseconds;
            try
            {
                if (PerformanceStats.Count > 0)
                {
                    TimeSinceFirstReduceInBatchCompletedMs = (now - PerformanceStats.Min(x => x.Value.Min(y => y.LevelStats.Count > 0 ? y.LevelStats.Last().Completed : DateTime.MaxValue))).TotalMilliseconds;
                }
            }
            catch (Exception)
            {
            }
        }
    }
}
