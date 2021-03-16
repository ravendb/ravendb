using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14809 : RavenTestBase
    {
        public RavenDB_14809(ITestOutputHelper output) : base(output)
        {
        }

        private class MapReduceWithOutputToCollection : AbstractIndexCreationTask<Row, FirstOutput>
        {
            public override string IndexName => "MapReduceWithOutputToCollection";

            public MapReduceWithOutputToCollection()
            {
                Map = rows => from row in rows
                                  // the below are not working
                                  // let doc = LoadDocument<Data>(row.DataId, $"{nameof(Data)}s")
                              let doc = LoadDocument<Data>(row.DataId, $"Datas{3}")

                              // the below works as expected
                              // let doc = LoadDocument<Data>(row.DataId)
                              select new FirstOutput
                              {
                                  Name = doc.Name,
                                  Email = "Email",
                                  LineNumber = row.LineNumber
                              };

                Reduce = results => from result in results
                                    group result by new { result.LineNumber } into g
                                    select new
                                    {
                                        Name = g.Select(x => x.Name).FirstOrDefault(),
                                        Email = g.Select(x => x.Email).FirstOrDefault(),
                                        LineNumber = g.Key.LineNumber
                                    };

                OutputReduceToCollection = @"FirstOutput";
            }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => new MapReduceWithOutputToCollection().Execute(store));
                Assert.Contains("Invalid argument in LoadDocument", e.ToString());
            }
        }

        private class Row
        {
            public string DataId { get; set; }
            public int LineNumber { get; set; }
        }

        private class Data
        {
            public string Name { get; set; }
        }

        private class FirstOutput
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public int LineNumber { get; set; }
        }
    }
}
