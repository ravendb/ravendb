// -----------------------------------------------------------------------
//  <copyright file="DatabaseMonitoring.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring
{
    public class DatabaseMetrics
    {
        public string DatabaseName { get; set; }
        public string DatabaseId { get; set; }
        public int UptimeInSec { get; set; }
        public double? TimeSinceLastBackupInSec { get; set; }
        
        public DatabaseCounts Counts { get; set; }
        public DatabaseStatistics Statistics { get; set; } 
        public DatabaseIndexesMetrics Indexes { get; set; }
        public DatabaseStorageMetrics Storage { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(UptimeInSec)] = UptimeInSec,
                [nameof(TimeSinceLastBackupInSec)] = TimeSinceLastBackupInSec,
                [nameof(Counts)] = Counts.ToJson(),
                [nameof(Statistics)] = Statistics.ToJson(),
                [nameof(Indexes)] = Indexes.ToJson(),
                [nameof(Storage)] = Storage.ToJson()
            };
        }
    }

    public class DatabaseStatistics
    {
        public double DocPutsPerSec { get; set; }
        public double MapIndexIndexesPerSec { get; set; }
        public double MapReduceIndexMappedPerSec { get; set; }
        public double MapReduceIndexReducedPerSec { get; set; }
        public double RequestsPerSec { get; set; }
        public int RequestsCount { get; set; }
        public double RequestAverageDuration { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocPutsPerSec)] = DocPutsPerSec,
                [nameof(MapIndexIndexesPerSec)] = MapIndexIndexesPerSec,
                [nameof(MapReduceIndexMappedPerSec)] = MapReduceIndexMappedPerSec,
                [nameof(MapReduceIndexReducedPerSec)] = MapReduceIndexReducedPerSec,
                [nameof(RequestsPerSec)] = RequestsPerSec,
                [nameof(RequestsCount)] = RequestsCount,
                [nameof(RequestAverageDuration)] = RequestAverageDuration
            };
        }
    }

    public class DatabaseCounts
    {
        public long Documents { get; set; }
        public long Revisions { get; set; }
        public long Attachments { get; set; }
        public long UniqueAttachments { get; set; }
        public long Alerts { get; set; }
        public int Rehabs { get; set; }
        public long PerformanceHints { get; set; }
        public int ReplicationFactor { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Documents)] = Documents,
                [nameof(Revisions)] = Revisions,
                [nameof(Attachments)] = Attachments,
                [nameof(UniqueAttachments)] = UniqueAttachments,
                [nameof(Alerts)] = Alerts,
                [nameof(Rehabs)] = Rehabs,
                [nameof(PerformanceHints)] = PerformanceHints,
                [nameof(ReplicationFactor)] = ReplicationFactor
            };
        }
    }
    
    public class DatabaseIndexesMetrics
    {
        public long Count { get; set; }
        public int StaleCount { get; set; }
        public long ErrorsCount { get; set; }
        public int StaticCount { get; set; }
        public int AutoCount { get; set; }
        public int IdleCount { get; set; }
        public int DisabledCount { get; set; }
        public int ErroredCount { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(StaleCount)] = StaleCount,
                [nameof(ErrorsCount)] = ErrorsCount,
                [nameof(StaticCount)] = StaticCount,
                [nameof(AutoCount)] = AutoCount,
                [nameof(IdleCount)] = IdleCount,
                [nameof(DisabledCount)] = DisabledCount,
                [nameof(ErroredCount)] = ErroredCount
            };
        }
    }

    public class DatabaseStorageMetrics
    {
        public long DocumentsAllocatedDataFileInMb { get; set; }
        public long DocumentsUsedDataFileInMb { get; set; }
        public long IndexesAllocatedDataFileInMb { get; set; }
        public long IndexesUsedDataFileInMb { get; set; }
        public long TotalAllocatedStorageFileInMb { get; set; }
        public long TotalFreeSpaceInMb { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentsAllocatedDataFileInMb)] = DocumentsAllocatedDataFileInMb,
                [nameof(DocumentsUsedDataFileInMb)] = DocumentsUsedDataFileInMb,
                [nameof(IndexesAllocatedDataFileInMb)] = IndexesAllocatedDataFileInMb,
                [nameof(IndexesUsedDataFileInMb)] = IndexesUsedDataFileInMb,
                [nameof(TotalAllocatedStorageFileInMb)] = TotalAllocatedStorageFileInMb,
                [nameof(TotalFreeSpaceInMb)] = TotalFreeSpaceInMb
            };
        }
    }
    
    public class DatabasesMetrics
    {
        public List<DatabaseMetrics> Results { get; set; } = new List<DatabaseMetrics>();
        public string PublicServerUrl { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(PublicServerUrl)] = PublicServerUrl,
                [nameof(Results)] = Results.Select(x => x.ToJson()).ToList()
            };
        }
    }
}
