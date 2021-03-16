// -----------------------------------------------------------------------
//  <copyright file="MemoryUsageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;
using Sparrow.Utils;
using Voron.Impl;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class MemoryUsageWidget : Widget
    {
        private readonly RavenServer _server;
        private readonly LowMemoryMonitor _lowMemoryMonitor = new();
        private readonly Action<IDynamicJson> _onMessage;
        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(5);

        public MemoryUsageWidget(int id, RavenServer server, Action<IDynamicJson> onMessage, CancellationToken shutdown) : base(id, shutdown)
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
                Time = SystemTime.UtcNow,
                LowMemorySeverity = LowMemoryNotification.Instance.IsLowMemory(memoryInfo, _lowMemoryMonitor, out _),
                PhysicalMemory = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                WorkingSet = memoryInfo.WorkingSet.GetValue(SizeUnit.Bytes),
                ManagedAllocations = managedMemoryInBytes,
                UnmanagedAllocations = totalUnmanagedAllocations,
                SystemCommitLimit = memoryInfo.TotalCommittableMemory.GetValue(SizeUnit.Bytes),
                EncryptionBuffersInUse = encryptionBuffers.CurrentlyInUseSize,
                EncryptionBuffersPool = encryptionBuffers.TotalPoolSize,
                MemoryMapped = totalMapping,
                DirtyMemory = dirtyMemoryState.TotalDirtyInBytes,
                AvailableMemory = memoryInfo.AvailableMemory.GetValue(SizeUnit.Bytes),
                AvailableMemoryForProcessing = memoryInfo.AvailableMemoryForProcessing.GetValue(SizeUnit.Bytes)
            };
        }
    }
}
