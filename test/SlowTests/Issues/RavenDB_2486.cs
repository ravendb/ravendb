using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2486 : RavenTestBase
    {
        private class Foo
        {
            public string Name { get; set; }
        }

        private class Index1 : AbstractIndexCreationTask<Foo>
        {
            public Index1()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  Name = foo.Name + "A"
                              };
            }
        }

        private class Index2 : AbstractIndexCreationTask<Foo>
        {
            public Index2()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  Name = foo.Name + "B"
                              };
                Analyze(x => x.Name, "NotExistingAnalyzerClassName");
            }
        }

        private class Index3 : AbstractIndexCreationTask<Foo>
        {
            public Index3()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  Name = foo.Name + "C"
                              };
            }
        }

        [Fact]
        public void Multiple_indexes_created_with_not_existing_analyzer_should_skip_only_the_invalid_index()
        {
            using (var store = GetDocumentStore())
            {
                try
                {
                    IndexCreation.CreateIndexes(new AbstractIndexCreationTask[] { new Index1(), new Index2(), new Index3() }, store);
                }
                catch (IndexCompilationException e)
                {
                    Assert.Contains("Index2", e.Message);
                }

                var indexInfo = store.Admin.Send(new GetStatisticsOperation()).Indexes;
                Assert.Equal(2, indexInfo.Length);
                Assert.True(indexInfo.Any(index => index.Name.Equals("Index1")));
                Assert.True(indexInfo.Any(index => index.Name.Equals("Index3")));
            }
        }

        [Fact]
        public async Task Multiple_indexes_created_withAsync_AndWith_not_existing_analyzer_should_skip_only_the_invalid_index()
        {
            using (var store = GetDocumentStore())
            {
                try
                {
                    await IndexCreation.CreateIndexesAsync(new AbstractIndexCreationTask[] { new Index1(), new Index2(), new Index3() }, store);
                }
                catch (AggregateException e)
                {
                    Assert.Contains("Index2", e.InnerExceptions.First().Message);
                }

                var indexInfo = store.Admin.Send(new GetStatisticsOperation()).Indexes;
                Assert.Equal(2, indexInfo.Length);
                Assert.True(indexInfo.Any(index => index.Name.Equals("Index1")));
                Assert.True(indexInfo.Any(index => index.Name.Equals("Index3")));
            }
        }
    }
}
