using System;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron.Global;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_13528 : StorageTest
    {
        public RavenDB_13528(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public unsafe void Can_write_more_than_4GB_to_journal_file()
        {
            var size = 4_780_343_296L;

            var ptr = PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size , out var stats);
            try
            {

                RequireFileBasedPager();

                using (var writer = Env.Options.CreateJournalWriter(10, size))
                {
                    Pal.jounral_entry entry = new() { Base = ptr, NumberOf4Kbs = (int)(size / 4096) };
                    writer.Write(1, new[]{entry}, entry.NumberOf4Kbs);
                }
            }
            finally
            {
                PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, size, stats);
            }
        }
    }
}
