using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Static;
using SlowTests.Bugs.Caching;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16889 : RavenTestBase
    {
        public RavenDB_16889(ITestOutputHelper output) : base(output)
        {
        }

        public class TestIndex : AbstractIndexCreationTask<TestObj>
        {
            public TestIndex()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        Count = taggable.Tags
                            .SelectMany((v => v.Value))
                            .Count()
                    };
            }
        }

        public class TestIndex2 : AbstractIndexCreationTask<TestObj>
        {
            public TestIndex2()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        Count = taggable.Tags
                            .SelectMany((v,i) => v.Value)
                            .Count()
                    };
            }
        }

        public class TestIndex3 : AbstractIndexCreationTask<TestObj>
        {
            public TestIndex3()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        Count = taggable.Tags
                            .SelectMany((v, i) => v.Value, (pair, valuePair) => valuePair)
                            .Count()
                    };
            }
        }
        public class TestIndex4 : AbstractIndexCreationTask<TestObj>
        {
            public TestIndex4()
            {
                Map = taggables =>
                    from taggable in taggables
                    select new
                    {
                        Count = taggable.Tags
                            .SelectMany(v => v.Value, (pair, valuePair) => valuePair)
                            .Count()
                    };
            }
        }

        [Fact]
        public async Task TestCase()
        {
            using var store = GetDocumentStore();
            var index = new TestIndex4();
            await index.ExecuteAsync(store);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj
                {
                    Tags = new Dictionary<string, Dictionary<string, string>>
                    {
                        {
                            "key1", new Dictionary<string, string>
                            {
                                {"key2", "value1" }
                            }
                        }
                    }
                });
                await session.SaveChangesAsync();
            }
            WaitForIndexing(store);
            WaitForUserToContinueTheTest(store);

            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }))
                .SelectMany(e => e.Errors)
                .Select(e => e.Error)
                .ToArray();
            var errorsString = string.Join("\n", errors);
            dynamic dic = new Dictionary<string,string>();
            Enumerable.Count((Dictionary<dynamic,dynamic>)dic);
            Assert.DoesNotContain("Failed to execute mapping function", errorsString);

            //var errors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            //Assert.True(errors.Any() == false, string.Join<IndexErrors>('\n', errors));

        }

        public class TestObj
        {
            public Dictionary<string, Dictionary<string, string>> Tags;
        }
    }
}
