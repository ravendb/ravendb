using System.Diagnostics;
using System;
using System.IO;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.DiskStatsGetter;
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

        [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Monitoring, RavenPlatform.Linux | RavenPlatform.Windows)]
        public async Task DeviceIoSampler_reads_a_real_device()
        {
            RequireFileBasedPager(); // a memory-only environment never writes to the device
            var deviceId = DataFileDeviceId();

            var sampler = DeviceIoSampler.ForDevice(deviceId, Env.Options.BasePath.FullPath);
            if (sampler == null)
            {
                Output.WriteLine($"device {deviceId}: no counters");
                return; // no counters for this device (a container without sysfs): the fallback path, nothing to assert
            }

            // the monitoring finds the device by path: it must reach the sampler of the data file's device
            Assert.Same(sampler, DeviceIoSampler.ForPath(Env.Options.BasePath.FullPath));

            var stopwatch = Stopwatch.StartNew();
            while (sampler.Current == null)
            {
                Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(10), "the sampler took no sample in 10 seconds");
                await Task.Delay(100);
            }

            var current = sampler.Current;
            Output.WriteLine($"device {deviceId}: {current}");
            Assert.True(current.QueueDepth >= 0 && current.WritesPerSecond >= 0 && current.WriteLatencyMs >= 0, $"negative values: {current}");

            var stats = new SampledDiskStatsGetter().Get(Env.Options.BasePath.FullPath);
            Assert.NotNull(stats);
            Assert.True(stats.QueueLength >= 0, $"negative queue length: {stats}");

            if (PlatformDetails.RunningOnLinux == false)
                return;

            using var counters = DeviceCounters.LinuxSysfsCounters.TryCreate(deviceId);
            Assert.False(counters.TrySample(Stopwatch.GetTimestamp(), out _)); // the baseline

            // journal writes are O_DIRECT | O_DSYNC: each commit reaches the device
            for (var i = 0; i < 8; i++)
            {
                using var tx = Env.WriteTransaction();
                var tree = tx.CreateTree("qdepth");
                tree.Add("key-" + i, new byte[8192]);
                tx.Commit();
            }

            Assert.True(counters.TrySample(Stopwatch.GetTimestamp(), out var sample));
            Assert.True(sample.WritesPerSecond > 0, $"no writes reached the device: {sample}");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void FirstTouchHintIgnoresPagesOutsideTheFile()
        {
            RequireFileBasedPager();
            var state = Env.CurrentStateRecord.DataPagerState;
            foreach (var page in new[] { -1L, -64L, long.MinValue, long.MaxValue })
                Env.HintFirstTouch(state, page);
        }

        private unsafe ulong DataFileDeviceId()
        {
            var state = Env.CurrentStateRecord.DataPagerState;
            Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_pager_get_device_id(state.Handle, out var deviceId, out _));
            return deviceId;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void DeviceWriteBudget_selects_drain_mode_and_returns_to_trickle()
        {
            var gate = new global::Voron.Impl.Journal.DeviceWriteBudget(sampler: null, pathOnDevice: "test",
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
            var gate = new global::Voron.Impl.Journal.DeviceWriteBudget(sampler: null, pathOnDevice: "test",
                syncCostThresholdTicks: TimeSpan.FromMilliseconds(100).Ticks, queueDepthThreshold: 5,
                classifyAboveLatencyTicks: TimeSpan.FromMilliseconds(2).Ticks);

            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Unknown, gate.MeasuredDeviceClass);

            // NVMe-shaped: 8KB writes at 100us. Small writes, but no budgeted volume is this fast.
            for (var i = 0; i < 16; i++)
                gate.RecordJournalWrite(TimeSpan.FromMicroseconds(100).Ticks, 8 * 1024, Stopwatch.GetTimestamp());
            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Fast, gate.MeasuredDeviceClass);

            // gp3-shaped: 12KB writes at 1.5ms. Small writes must NOT classify - a small write can be
            // fast on a slow device, so only the decisively-low band is trusted below the 256KB size gate.
            var gp3 = new global::Voron.Impl.Journal.DeviceWriteBudget(sampler: null, pathOnDevice: "test",
                syncCostThresholdTicks: TimeSpan.FromMilliseconds(100).Ticks, queueDepthThreshold: 5,
                classifyAboveLatencyTicks: TimeSpan.FromMilliseconds(2).Ticks);
            for (var i = 0; i < 16; i++)
                gp3.RecordJournalWrite(TimeSpan.FromMilliseconds(1.5).Ticks, 12 * 1024, Stopwatch.GetTimestamp());
            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Unknown, gp3.MeasuredDeviceClass);

            // large writes above the size gate classify by the ordinary threshold: 9ms EWMA => Budgeted
            for (var i = 0; i < 16; i++)
                gp3.RecordJournalWrite(TimeSpan.FromMilliseconds(9).Ticks, 512 * 1024, Stopwatch.GetTimestamp());
            Assert.Equal(global::Voron.Impl.Journal.DeviceWriteBudget.DeviceClass.Budgeted, gp3.MeasuredDeviceClass);
        }
    }
}
