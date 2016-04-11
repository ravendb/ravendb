using System;
using System.Collections.Generic;
using System.Text;
using FastTests.Voron.FixedSize;
using Sparrow;
using Xunit;
using Voron.Data.RawData;
using Voron;
using Voron.Impl;

namespace FastTests.Voron.RawData
{
    public unsafe class SmallDataSection : StorageTest
    {
        [Fact]
        public void CanReadAndWriteFromSection()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long id;
            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                
                AssertValueMatches(section,id, "Hello There");
            }
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void CanAllocateMultipleValues(int seed)
        {
            var random = new Random(seed);

            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");
                pageNumber = section.PageNumber;
                tx.Commit();
            }
            var dic = new Dictionary<long, int>();
            for (int i = 0; i < 100; i++)
            {
                long id;
                using (var tx = Env.WriteTransaction())
                {
                    var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                    Assert.True(section.TryAllocate(random.Next(16,256), out id));
                    WriteValue(section, id, i.ToString("0000000000000"));
                    dic[id] = i;
                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                    AssertValueMatches(section, id, i.ToString("0000000000000"));
                }
            }

            foreach (var kvp in dic)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                    AssertValueMatches(section, kvp.Key, kvp.Value.ToString("0000000000000"));
                }
            }
        }

        [Fact]
        public void CanAllocateEnoughToFillEntireSection()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                int allocationSize = 1020;
                int allocations = section.Size / allocationSize;

                int i = 0;
                var rnd = new Random();
                int selected = rnd.Next(allocations - 2);

                long id, idToFree = 0;

                while (section.TryAllocate(allocationSize, out id))
                {
                    if (i == selected)
                        idToFree = id;

                    i++;
                }

                Assert.False(section.TryAllocate(allocationSize, out id));

                section.Free(idToFree);

                Assert.True(section.TryAllocate(allocationSize, out id));
            }
        }

        [Fact]
        public void CanReadAndWriteFromSection_SingleTx()
        {
            Env.Options.ManualFlushing = true;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");

                long id;
            
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
          

                AssertValueMatches(section, id, "Hello There");

                tx.Commit();
            }
            Env.FlushLogToDataFile();
        }


        [Fact]
        public void CanReadAndWriteFromSection_AfterFlush()
        {
            Env.Options.ManualFlushing = true;
            long pageNumber;
            long id;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");
                pageNumber = section.PageNumber;
          
                //var section = new RawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
                tx.Commit();
            }
            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                AssertValueMatches(section, id, "Hello There");
            }
        }

        [Fact]
        public void ShouldNotReturnMoreIdsThanTotalNumberOfEntriesInSection()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long newId;

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                Assert.True(section.TryAllocate(16, out newId));
                WriteValue(section, newId, 1.ToString("0000000000000"));
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                var ids = section.GetAllIdsInSectionContaining(newId);

                Assert.Equal(section.NumberOfEntries, ids.Count);
                Assert.Equal(1, ids.Count);
                Assert.Equal(newId, ids[0]);

                AssertValueMatches(section, newId, 1.ToString("0000000000000"));
            }
        }

        [Fact]
        public void ShouldReturnValidIdsOfEntriesInSectionThatAreReadable()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, "test");
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long idWhichIsGoingToBeDeleted1;
            long idWhichIsGoingToBeDeleted2;
            long existingId;

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                Assert.True(section.TryAllocate(2000, out idWhichIsGoingToBeDeleted1));
                WriteValue(section, idWhichIsGoingToBeDeleted1, 1.ToString("0000000000000"));
                Assert.True(section.TryAllocate(2000, out idWhichIsGoingToBeDeleted2));
                WriteValue(section, idWhichIsGoingToBeDeleted2, 2.ToString("0000000000000"));
                
                Assert.True(section.TryAllocate(2000, out existingId));
                WriteValue(section, existingId, 3.ToString("0000000000000"));
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                section.Free(idWhichIsGoingToBeDeleted1);
                section.Free(idWhichIsGoingToBeDeleted2);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                var ids = section.GetAllIdsInSectionContaining(existingId);

                Assert.Equal(1, ids.Count);
                Assert.Equal(existingId, ids[0]);

                AssertValueMatches(section, existingId, 3.ToString("0000000000000"));
            }
        }

        private static void WriteValue(ActiveRawDataSmallSection section, long id, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            fixed (byte* p = bytes)
            {
                Assert.True(section.TryWrite(id, p, bytes.Length));
            }
        }

        private static void AssertValueMatches(ActiveRawDataSmallSection section, long id, string expected)
        {
            int size;
            var p = section.DirectRead(id, out size);
            var buffer = new byte[size];
            fixed (byte* bp = buffer)
            {
                Memory.Copy(bp, p, size);
            }
            var actual = Encoding.UTF8.GetString(buffer, 0, size);
            Assert.Equal(expected,actual);
        }
    }
}