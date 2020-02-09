using System;
using AutoMapper;
using Lucene.Net.QueryParsers;
using Tests.Infrastructure;
using Vibrant.InfluxDB.Client;

namespace Tests.ResourceSnapshotAggregator
{
    public class ResourceUsageSnapshot
    {
        [InfluxTag(nameof(TestStage))]
        public TestStage TestStage { get; set; }

        [InfluxField(nameof(JobName))]
        public string JobName { get; set; }

        //note: using InfluxTag means data can be grouped by this field
        [InfluxTag(nameof(BuildNumber))]
        public string BuildNumber { get; set; }

        [InfluxTag(nameof(Comment))]
        public string Comment { get; set; }

        [InfluxTimestamp]
        public DateTime InfluxTimestamp => DateTime.Parse(TimeStamp);

        [InfluxField(nameof(TimeStamp))]
        public string TimeStamp { get; set; }
            
        [InfluxField(nameof(MachineCpuUsage))]
        public long MachineCpuUsage { get; set; }
            
        [InfluxField(nameof(ProcessCpuUsage))]
        public long ProcessCpuUsage { get; set; }
            
        [InfluxField(nameof(ProcessMemoryUsageInMb))]
        public long ProcessMemoryUsageInMb { get; set; }
            
        [InfluxField(nameof(TotalMemoryInMb))]
        public long TotalMemoryInMb { get; set; }
            
        [InfluxField(nameof(AvailableMemoryInMb))]
        public long AvailableMemoryInMb { get; set; }
            
        [InfluxField(nameof(TotalCommittableMemoryInMb))]
        public long TotalCommittableMemoryInMb { get; set; }
            
        [InfluxField(nameof(CurrentCommitChargeInMb))]
        public long CurrentCommitChargeInMb { get; set; }
            
        [InfluxField(nameof(SharedCleanMemoryInMb))]
        public long SharedCleanMemoryInMb { get; set; }
            
        [InfluxField(nameof(TotalScratchDirtyMemory))]
        public long TotalScratchDirtyMemory { get; set; } //
            
        [InfluxField(nameof(TotalScratchAllocatedMemory))]
        public long TotalScratchAllocatedMemory { get; set; } //
            
        [InfluxField(nameof(TotalDirtyMemory))]
        public long TotalDirtyMemory { get; set; } //
            
        [InfluxField(nameof(IsHighDirty))]
        public bool IsHighDirty { get; set; }

        public ResourceUsageSnapshot()
        {
        }

        private static readonly Mapper _mapper = new Mapper(new MapperConfiguration(cfg => cfg.AddMaps(typeof(TestResourceSnapshotWriter.TestResourceSnapshot).Assembly, typeof(ResourceUsageSnapshot).Assembly)));

        public static ResourceUsageSnapshot From(TestResourceSnapshotWriter.TestResourceSnapshot testResourceSnapshot, string jobName, string buildNumber)
        {
            var resourceUsageSnapshot = _mapper.Map<TestResourceSnapshotWriter.TestResourceSnapshot, ResourceUsageSnapshot>(testResourceSnapshot);
            resourceUsageSnapshot.JobName = jobName;
            resourceUsageSnapshot.BuildNumber = buildNumber;
            return resourceUsageSnapshot;
        }

        public static ResourceUsageSnapshot operator-(ResourceUsageSnapshot snapshot1, ResourceUsageSnapshot snapshot2)
        {
            return new ResourceUsageSnapshot
            {
                JobName = snapshot2.JobName,
                BuildNumber = snapshot2.BuildNumber,
                TestStage = TestStage.Delta,
                Comment = snapshot2.Comment,
                TotalScratchDirtyMemory = Math.Abs(snapshot2.TotalScratchDirtyMemory - snapshot1.TotalScratchDirtyMemory),
                IsHighDirty = snapshot1.IsHighDirty || snapshot2.IsHighDirty,
                TotalDirtyMemory = Math.Abs(snapshot2.TotalDirtyMemory - snapshot1.TotalDirtyMemory),
                AvailableMemoryInMb = Math.Abs(snapshot2.AvailableMemoryInMb - snapshot1.AvailableMemoryInMb),
                CurrentCommitChargeInMb = Math.Abs(snapshot2.CurrentCommitChargeInMb - snapshot1.CurrentCommitChargeInMb),
                MachineCpuUsage = Math.Abs(snapshot2.MachineCpuUsage - snapshot1.MachineCpuUsage),
                ProcessCpuUsage = Math.Abs(snapshot2.ProcessCpuUsage - snapshot1.MachineCpuUsage),
                ProcessMemoryUsageInMb = Math.Abs(snapshot2.ProcessMemoryUsageInMb - snapshot1.ProcessMemoryUsageInMb),
                SharedCleanMemoryInMb = Math.Abs(snapshot2.SharedCleanMemoryInMb - snapshot1.SharedCleanMemoryInMb),
                TimeStamp = snapshot2.TimeStamp,
                TotalCommittableMemoryInMb = Math.Abs(snapshot2.TotalCommittableMemoryInMb - snapshot1.TotalCommittableMemoryInMb),
                TotalMemoryInMb = Math.Abs(snapshot2.TotalMemoryInMb - snapshot1.TotalMemoryInMb),
                TotalScratchAllocatedMemory = Math.Abs(snapshot2.TotalScratchAllocatedMemory - snapshot1.TotalScratchAllocatedMemory)
            };
        }
    }
}
