using System;
using System.IO;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace FastTests.Voron
{
    public class RavenDB_27375 : StorageTest
    {
        public RavenDB_27375(ITestOutputHelper output) : base(output)
        {
        }

        [LinuxFact]
        public unsafe void DeviceQueueDepthReader_reads_a_real_device()
        {
            var state = Env.CurrentStateRecord.DataPagerState;
            Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_pager_get_device_id(state.Handle, out var deviceId, out _));

            using var reader = DeviceQueueDepthReader.TryCreate(Env.Options.BasePath.FullPath, deviceId);
            if (reader == null)
                return; // container without sysfs - the fallback path, nothing to assert

            // generate some write traffic so the counters move
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("qdepth");
                var buffer = new byte[8192];
                for (var i = 0; i < 512; i++)
                    tree.Add("key-" + i, buffer);
                tx.Commit();
            }

            var first = reader.Read();
            Assert.True(first >= 0, $"queue depth must not be negative, got {first}");

            var second = reader.Read();
            Assert.True(second >= 0, $"queue depth must not be negative, got {second}");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void DeviceWriteBudget_selects_drain_mode_and_returns_to_trickle()
        {
            var gate = new global::Voron.Impl.Journal.DeviceWriteBudget(queueReader: null, pathOnDevice: "test",
                syncCostThresholdTicks: TimeSpan.FromMilliseconds(100).Ticks, queueDepthThreshold: 5,
                classifyAboveLatencyTicks: TimeSpan.FromMilliseconds(2).Ticks);

            // healthy barrier: trickle mode
            gate.RecordSyncCost(TimeSpan.FromMilliseconds(5).Ticks);
            Assert.False(gate.ShouldDrain());

            // expensive barrier: drain mode (the queue signal is absent, the barrier decides)
            for (var i = 0; i < 8; i++)
                gate.RecordSyncCost(TimeSpan.FromMilliseconds(800).Ticks);
            Assert.True(gate.ShouldDrain());

            // still busy: stays in drain mode
            Assert.True(gate.ShouldDrain());
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void DeviceWriteBudget_classifies_fast_device_from_small_writes_when_latency_is_decisive()
        {
            var gate = new global::Voron.Impl.Journal.DeviceWriteBudget(queueReader: null, pathOnDevice: "test",
                syncCostThresholdTicks: TimeSpan.FromMilliseconds(100).Ticks, queueDepthThreshold: 5,
                classifyAboveLatencyTicks: TimeSpan.FromMilliseconds(2).Ticks);

            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Unknown, gate.MeasuredDeviceClass);

            // NVMe-shaped: 8KB writes at 100us. Small writes, but no budgeted volume is this fast.
            for (var i = 0; i < 16; i++)
                gate.RecordJournalWrite(TimeSpan.FromMicroseconds(100).Ticks, 8 * 1024);
            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Fast, gate.MeasuredDeviceClass);

            // gp3-shaped: 12KB writes at 1.5ms. Small writes must NOT classify - a small write can be
            // fast on a slow device, so only the decisively-low band is trusted below the 256KB size gate.
            var gp3 = new global::Voron.Impl.Journal.DeviceWriteBudget(queueReader: null, pathOnDevice: "test",
                syncCostThresholdTicks: TimeSpan.FromMilliseconds(100).Ticks, queueDepthThreshold: 5,
                classifyAboveLatencyTicks: TimeSpan.FromMilliseconds(2).Ticks);
            for (var i = 0; i < 16; i++)
                gp3.RecordJournalWrite(TimeSpan.FromMilliseconds(1.5).Ticks, 12 * 1024);
            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Unknown, gp3.MeasuredDeviceClass);

            // large writes above the size gate classify by the ordinary threshold: 9ms EWMA => Budgeted
            for (var i = 0; i < 16; i++)
                gp3.RecordJournalWrite(TimeSpan.FromMilliseconds(9).Ticks, 512 * 1024);
            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Budgeted, gp3.MeasuredDeviceClass);
        }
    }
}
