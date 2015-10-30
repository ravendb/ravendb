using System;
using System.Text;
using Voron.Data.RawData;
using Xunit;

namespace Voron.Tests.RawData
{
    public unsafe class SmallDataSection : StorageTest
    {
        [Fact]
        public void CanReadAndWriteFromSection()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var section = RawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
                tx.Commit();
            }

            long id;
            using (var tx = Env.WriteTransaction())
            {
                var section = new RawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var section = new RawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                
                AssertValueMatches(section,id, "Hello There");
            }
        }

        [Fact]
        public void CanReadAndWriteFromSection_SingleTx()
        {
            Env.Options.ManualFlushing = true;
            using (var tx = Env.WriteTransaction())
            {
                var section = RawDataSmallSection.Create(tx.LowLevelTransaction);

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
                var section = RawDataSmallSection.Create(tx.LowLevelTransaction);
                pageNumber = section.PageNumber;
          
                //var section = new RawDataSmallSection(tx.LowLevelTransaction, pageNumber);
                Assert.True(section.TryAllocate(15, out id));
                WriteValue(section, id, "Hello There");
                tx.Commit();
            }
            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var section = new RawDataSmallSection(tx.LowLevelTransaction, pageNumber);

                AssertValueMatches(section, id, "Hello There");
            }
        }

        private static void WriteValue(RawDataSmallSection section, long id, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            fixed (byte* p = bytes)
            {
                Assert.True(section.TryWrite(id, p, bytes.Length));
            }
        }

        private static void AssertValueMatches(RawDataSmallSection section, long id, string expected)
        {
            int size;
            var p = section.DirectRead(id, out size);
            var chars = new char[Encoding.UTF8.GetMaxCharCount(size)];
            fixed (char* c = chars)
            {
                int bytesUsed;
                int charsUsed;
                bool completed;
                Encoding.UTF8.GetDecoder().Convert(p, size, c, chars.Length, true, out bytesUsed, out charsUsed, out completed);
                Assert.Equal(expected, new string(chars, 0, charsUsed));
            }
        }
    }
}