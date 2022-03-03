using System;
using System.Runtime.InteropServices;
using FastTests.Voron;
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

        [MultiplatformFact(RavenArchitecture.AllX64)]
        public unsafe void Can_write_more_than_4GB_to_journal_file()
        {
            var size = 4_780_343_296L;
            long _4Kb = 4L * Constants.Size.Kilobyte;

            size = size + (_4Kb - (size % _4Kb));

            var ptr = Marshal.AllocHGlobal(new IntPtr(size + _4Kb));
            try
            {
                var pos = ptr.ToInt64() + (_4Kb - (ptr.ToInt64() % _4Kb));

                RequireFileBasedPager();

                var writer = Env.Options.CreateJournalWriter(10, size);
                writer.Write(1, (byte*)pos, (int)(size / _4Kb));
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
        }
    }
}
