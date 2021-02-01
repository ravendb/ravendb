// -----------------------------------------------------------------------
//  <copyright file="IndexesMetrics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring
{
    public class IndexMetrics
    {
        public string DatabaseName { get; set; }
        public string IndexName { get; set; }
        public IndexPriority Priority { get; set; }
        public IndexState State { get; set; }
        public int Errors { get; set; }
        public double? TimeSinceLastQueryInSec { get; set; }
        public double? TimeSinceLastIndexingInSec { get; set; }
        public IndexLockMode LockMode { get; set; }
        public bool IsInvalid { get; set; }
        public IndexRunningStatus Status { get; set; }
        public double MappedPerSec { get; set; }
        public double ReducedPerSec { get; set; }
        public IndexType Type { get; set; }
        public int EntriesCount { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(IndexName)] = IndexName,
                [nameof(Priority)] = Priority,
                [nameof(State)] = State,
                [nameof(Errors)] = Errors,
                [nameof(TimeSinceLastQueryInSec)] = TimeSinceLastQueryInSec,
                [nameof(TimeSinceLastIndexingInSec)] = TimeSinceLastIndexingInSec,
                [nameof(LockMode)] = LockMode,
                [nameof(IsInvalid)] = IsInvalid,
                [nameof(Status)] = Status,
                [nameof(MappedPerSec)] = MappedPerSec,
                [nameof(ReducedPerSec)] = ReducedPerSec,
                [nameof(Type)] = Type
            };
        }
    }
    
    public class IndexesMetrics
    {
        public List<IndexMetrics> Results { get; set; } = new List<IndexMetrics>();
        
        public string PublicServerUrl { get; set; }
        
        public string NodeTag { get; set; }
    }
}
