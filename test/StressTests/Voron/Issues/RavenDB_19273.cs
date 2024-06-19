using System.Runtime.InteropServices;
using System;
using FastTests.Voron;
using Sparrow;
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
        long _4Kb = 4L * Constants.Size.Kilobyte;

        size = size + (_4Kb - (size % _4Kb));

        ulong hash1;

        var ptr = Marshal.AllocHGlobal(new IntPtr(size + _4Kb));

        try
        {
            var pos = ptr.ToInt64() + (_4Kb - (ptr.ToInt64() % _4Kb));
            var p = (byte*)pos;

            for (long i = 0; i < size; i += 100 * Constants.Size.Megabyte)
            {
                Memory.Set(p + i, 133, 1);
            }

            hash1 = Hashing.XXHash64.Calculate(p, (ulong)size, 1);

            RequireFileBasedPager();

            using (var writer = Env.Options.CreateJournalWriter(10, size))
                writer.Write(1, (byte*)pos, (int)(size / _4Kb));
        }
        finally
        {
            Marshal.FreeHGlobal(ptr);
        }

        var (pager,state) = Env.Options.OpenJournalPager(10, default);
        using (pager)
        using (var tx = Env.ReadTransaction())
        {
            var readPtr = pager.AcquirePagePointer(state,ref tx.LowLevelTransaction.PagerTransactionState, 0);

            readPtr += _4Kb;

            var hash2 = Hashing.XXHash64.Calculate(readPtr, (ulong)size, 1);

            Assert.Equal(hash1, hash2);
        }
    }
}
