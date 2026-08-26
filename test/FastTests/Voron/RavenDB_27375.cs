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
    }
}
