// -----------------------------------------------------------------------
//  <copyright file="MemoryUsageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
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
        private readonly LowMemoryMonitor _lowMemoryMonitor = new LowMemoryMonitor();
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
            var extendedData = PrepareExtendedData();

            // send all data at the beginning 
            _onMessage(extendedData);

            for (var i = 0; i < 8; i++)
            {
                await WaitOrThrowOperationCanceled(_defaultInterval);
                // minor update - send only basic info - to avoid costly calculation 
                var baseData = PrepareBasicData();
                _onMessage(baseData);
            }
            
            await WaitOrThrowOperationCanceled(_defaultInterval);
        }

        private MemoryBasicUsagePayload PrepareBasicData()
        {
            var memoryInfo = _server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfo);
            
            return new MemoryBasicUsagePayload()
            {
                LowMemorySeverity = LowMemoryNotification.Instance.IsLowMemory(memoryInfo, _lowMemoryMonitor, out _),
                PhysicalMemory = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                WorkingSet = memoryInfo.WorkingSet.GetValue(SizeUnit.Bytes),
                AvailableMemory = memoryInfo.AvailableMemory.GetValue(SizeUnit.Bytes),
            };
        }
        
        private MemoryExtendedUsagePayload PrepareExtendedData()
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
            
            return new MemoryExtendedUsagePayload
            {
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

    public enum MemoryUsageType
    {
        Basic,
        Extended
    }
}
