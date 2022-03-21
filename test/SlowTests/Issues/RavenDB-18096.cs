using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18096 : RavenTestBase
    {
        public RavenDB_18096(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Parse_Numeric_Value_In_Index()
        {
            using (var store = GetDocumentStore())
            {
                await new DocumentIndex().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Short = 0
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session.Query<DocumentIndex.Result, DocumentIndex>()
                        .OfType<Document>()
                        .ToListAsync();

                    Assert.Equal(1, docs.Count);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }

            public short Short { get; set; }
        }

        private class DocumentIndex : AbstractIndexCreationTask<Document>
        {
            public class Result
            {
                public long Long { get; set; }
                public int Int { get; set; }
                public short Short { get; set; }
                public ushort UShort { get; set; }
                public uint UInt { get; set; }
                public ulong Ulong { get; set; }
                public byte Byte { get; set; }
                public sbyte SByte { get; set; }
            }

            public DocumentIndex()
            {
                Map = documents => from document in documents
                    select new Result
                    {
                        Long = (long)document.Short,
                        Int = (int)document.Short,
                        Short = (short)document.Short,
                        Ulong = (ulong)document.Short,
                        UInt = (uint)document.Short,
                        UShort = (ushort)document.Short,
                        Byte = (byte)document.Short,
                        SByte = (sbyte)document.Short
                    };
            }
        }
    }
}
