using System.Runtime.InteropServices;
using System;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Issues;

public class RavenDB_19273 : StorageTest
{
    public RavenDB_19273(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
    public unsafe void Will_write_correctly_more_than_2GB_to_journal_file()
    {
        var size = 4_780_343_296L;
        ulong hash1;

        var ptr = PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(size, out var stats);

        try
        {

            for (long i = 0; i < size; i += 100 * Constants.Size.Megabyte)
            {
                Memory.Set(ptr + i, 133, 1);
            }

            hash1 = Hashing.XXHash64.Calculate(ptr, (ulong)size, 1);

            RequireFileBasedPager();

            using (var writer = Env.Options.CreateJournalWriter(10, size))
            {
                Pal.jounral_entry entry = new() { Base = ptr, NumberOf4Kbs = (int)(size / 4096), };
                writer.Write(1, new[]{entry}, entry.NumberOf4Kbs);
            }
        }
        finally
        {
            PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, size, stats);
        }

        var (pager,state) = Env.Options.OpenJournalPager(10, default);
        using (pager)
        using (var tx = Env.ReadTransaction())
        {
            var readPtr = pager.AcquirePagePointer(state,ref tx.LowLevelTransaction.PagerTransactionState, 0);

            readPtr += 4096;

            var hash2 = Hashing.XXHash64.Calculate(readPtr, (ulong)size, 1);

            Assert.Equal(hash1, hash2);
        }
    }
}
