using System;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_11865 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxScratchBufferSize = 2 * Constants.Size.Megabyte;
        }

        [Fact]
        public void Scratch_files_on_recyclable_area_should_be_deleted_on_dispose()
        {
            RequireFileBasedPager();

            var r = new Random(1);

            var bytes = new byte[1024];

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    r.NextBytes(bytes);

                    tx.CreateTree("items").Add($"item/{i}", new MemoryStream(bytes));

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    r.NextBytes(bytes);

                    tx.CreateTree("items").Add($"item/{i}", new MemoryStream(bytes));

                    tx.Commit();
                }
            }

            Env.Dispose();

            var temp = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).TempPath.FullPath;

            var scratches = new DirectoryInfo(temp).GetFiles("scratch.*");

            Assert.Equal(0, scratches.Length);
        }
    }
}
