using System;
using System.IO;
using System.Text;
using Raven.Server.Platform.Posix;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_17185 : NoDisposalNeeded
    {
        public RavenDB_17185(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanParseMemInfo()
        {
            MemInfo memInfo;
            using (var stream = GetContent("RavenDB_17185.memInfo.txt"))
                memInfo = MemInfoReader.Read(stream);

            Assert.Equal(32945836, memInfo.MemTotal.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(20364264, memInfo.MemFree.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(34032, memInfo.Buffers.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(188576, memInfo.Cached.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.SwapCached.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(167556, memInfo.Active.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(157876, memInfo.Inactive.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(103104, memInfo.Active_anon.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(17440, memInfo.Inactive_anon.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(64452, memInfo.Active_file.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(140436, memInfo.Inactive_file.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.Unevictable.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.Mlocked.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(31921276, memInfo.SwapTotal.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(31788848, memInfo.SwapFree.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.Dirty.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.Writeback.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(102824, memInfo.AnonPages.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(71404, memInfo.Mapped.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(17720, memInfo.Shmem.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(13868, memInfo.Slab.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(6744, memInfo.SReclaimable.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(7124, memInfo.SUnreclaim.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(2848, memInfo.KernelStack.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(2524, memInfo.PageTables.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.NFS_Unstable.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.Bounce.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.WritebackTmp.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(515524, memInfo.CommitLimit.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(3450064, memInfo.Committed_AS.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(122880, memInfo.VmallocTotal.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(21296, memInfo.VmallocUsed.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(66044, memInfo.VmallocChunk.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.HardwareCorrupted.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(2048, memInfo.AnonHugePages.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.HugePages_Total.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.HugePages_Free.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.HugePages_Rsvd.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(0, memInfo.HugePages_Surp.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(2048, memInfo.Hugepagesize.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(12280, memInfo.DirectMap4k.GetValue(SizeUnit.Kilobytes));
            Assert.Equal(897024, memInfo.DirectMap4M.GetValue(SizeUnit.Kilobytes));

            Assert.Equal(0, memInfo.Other.Count); // we do not want any unmapped values
        }

        [LinuxFact]
        public void CanParseMemInfo_Live()
        {
            var memInfo = MemInfoReader.Read();

            Assert.True(memInfo.MemTotal.GetValue(SizeUnit.Bytes) > 0);

            if (memInfo.Other.Count > 0)
            {
                var sb = new StringBuilder("Following properties were not mapped:");
                foreach (var kvp in memInfo.Other)
                    sb.AppendLine($"{kvp.Key}: {kvp.Value.GetValue(SizeUnit.Kilobytes)} kB");

                throw new InvalidOperationException(sb.ToString());
            }
        }

        private static Stream GetContent(string name)
        {
            var assembly = typeof(RavenDB_17185).Assembly;
            return assembly.GetManifestResourceStream("FastTests.Data." + name);
        }
    }
}
