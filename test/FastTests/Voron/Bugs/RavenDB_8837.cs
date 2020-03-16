using Raven.Server.Documents;
using Voron;
using Voron.Data.RawData;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_8837 : StorageTest
    {
        public RavenDB_8837(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void Braking_large_allocation_in_scratch_file_has_to_really_create_separate_pages_of_size_one()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx, "test", (byte)TableType.None);
                pageNumber = section.PageNumber;

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // just to increment transaction id
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx, pageNumber);

                section.DeleteSection(pageNumber);

                var allocatePage = tx.LowLevelTransaction.AllocatePage(1);

                // the case is that the free space handling will return same page number as we just freed by deleting raw data section
                // if the below assertion fails it means we have changed voron internals and this test might require adjustments

                Assert.Equal(allocatePage.PageNumber, pageNumber); 

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.WriteTransaction())
            {
                // just to increment transaction id
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // just to increment transaction id
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // ensure below call won't throw 'An item with the same key has already been added'
                // from ScratchBufferFile.BreakLargeAllocationToSeparatePages

                ActiveRawDataSmallSection.Create(tx, "test", (byte)TableType.None); 
                tx.Commit();
            }
        }
    }
}
