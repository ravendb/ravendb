// -----------------------------------------------------------------------
//  <copyright file="MemoryUsageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;
using Sparrow.Utils;
using Voron.Impl;

namespace Raven.Server.NotificationCenter.Widgets
{
    public class MemoryUsageWidget : Widget
    {
        private readonly RavenServer _server;
        private readonly Action<MemoryUsagePayload> _onMessage;
        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(5);

        public MemoryUsageWidget(int id, RavenServer server, Action<MemoryUsagePayload> onMessage, CancellationToken shutdown) : base(id, shutdown)
        {
            _server = server;
            _onMessage = onMessage;
        }

        public override WidgetType Type => WidgetType.MemoryUsage;

        protected override async Task DoWork()
        {
            var data = PrepareData();

            _onMessage(data);
            
            await WaitOrThrowOperationCanceled(_defaultInterval);
        }

        private MemoryUsagePayload PrepareData()
        {
            var memoryInfo = _server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended);
            long managedMemoryInBytes = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();
            long totalUnmanagedAllocations = NativeMemory.TotalAllocatedMemory;
            var encryptionBuffers = EncryptionBuffersPool.Instance.GetStats();
            var dirtyMemoryState = MemoryInformation.GetDirtyMemoryState();
            
            long totalMapping = 0;
            foreach (var mapping in NativeMemory.FileMapping)
            foreach (var singleMapping in mapping.Value.Value.Info)
            {
                totalMapping += singleMapping.Value;
            }
            
            return new MemoryUsagePayload
            {
                LowMemorySeverity = LowMemoryNotification.Instance.IsLowMemory(memoryInfo, new LowMemoryMonitor(), out _),
                PhysicalMemory = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                WorkingSet = memoryInfo.WorkingSet.GetValue(SizeUnit.Bytes),
                ManagedAllocations = managedMemoryInBytes,
                UnmanagedAllocations = totalUnmanagedAllocations,
                EncryptionBuffersInUse = encryptionBuffers.CurrentlyInUseSize,
                EncryptionBuffersPool = encryptionBuffers.TotalPoolSize,
                MemoryMapped = totalMapping,
                DirtyMemory = dirtyMemoryState.TotalDirtyInBytes,
                AvailableMemory = memoryInfo.AvailableMemory.GetValue(SizeUnit.Bytes),
                AvailableMemoryForProcessing = memoryInfo.AvailableMemoryForProcessing.GetValue(SizeUnit.Bytes)
            };
        }
    }
    
    public class MemoryUsagePayload : IDynamicJson
    {
        public LowMemorySeverity LowMemorySeverity { get; set; }
        public long PhysicalMemory { get; set; }
        public long WorkingSet { get; set; }
        public long ManagedAllocations { get; set; }
        public long UnmanagedAllocations { get; set; }
        public long EncryptionBuffersInUse { get; set; }
        public long EncryptionBuffersPool { get; set; }
        public long MemoryMapped { get; set; }
        public long DirtyMemory { get; set; }
        public long AvailableMemory { get; set; }
        public long AvailableMemoryForProcessing { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LowMemorySeverity)] = LowMemorySeverity,
                [nameof(PhysicalMemory)] = PhysicalMemory,
                [nameof(WorkingSet)] = WorkingSet,
                [nameof(ManagedAllocations)] = ManagedAllocations,
                [nameof(UnmanagedAllocations)] = UnmanagedAllocations,
                [nameof(EncryptionBuffersInUse)] = EncryptionBuffersInUse,
                [nameof(EncryptionBuffersPool)] = EncryptionBuffersPool,
                [nameof(MemoryMapped)] = MemoryMapped,
                [nameof(DirtyMemory)] = DirtyMemory,
                [nameof(AvailableMemory)] = AvailableMemory,
                [nameof(AvailableMemoryForProcessing)] = AvailableMemoryForProcessing
            };
        }
    }
}
