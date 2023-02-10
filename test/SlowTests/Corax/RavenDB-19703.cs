using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class RavenDB_19703 : RavenTestBase
    {
        public RavenDB_19703(ITestOutputHelper output) : base(output)
        {
        }

        private record Person(string Name);

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanStoreNullValueInsideAutoIndex(Options options)
        {
            using var store = GetDocumentStore(options);
            var person = new Person(Encodings.Utf8.GetString(new byte[] { (byte)'m', (byte)'a', (byte)'c', (byte)'\0', (byte)'e', (byte)'\0' }));
            using (var session = store.OpenSession())
            {
                session.Store(person);
                session.SaveChanges();

                var result = session.Advanced
                    .DocumentQuery<Person>()
                    .WaitForNonStaleResults()
                    .WhereEndsWith(i => i.Name, Encodings.Utf8.GetString(new byte[] { (byte)'\0' }))
                    .ToList();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(1, result.Count);
                Assert.Equal(person.Name, result[0].Name);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void FindOnRawDataWithNulls(Options options)
        {
            var rnd = new Random(1337);
            using var store = GetDocumentStore(options);
            {
                using var bulkInsert = store.BulkInsert();

                for (int i = 0; i < 1000; i++)
                {
                    byte[] data = new byte[rnd.Next(128) + 4];
                    rnd.NextBytes(data);
                    data[rnd.Next(data.Length - 1)] = 0;

                    bulkInsert.Store(new Item { Name = $"TestFirst{i}", Data = data});
                }
            }
            new NestedArray().Execute(store);
            Indexes.WaitForIndexing(store);

            using var session = store.OpenSession();
            var result = session.Query<Item, NestedArray>().Where(p => p.Data.Any(data => data == 0)).ToList();
            Assert.Equal(1000, result.Count);
            result = session.Query<Item, NestedArray>().Where(p => p.Data.Any(data => data == 0)).ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Equal(1000, result.Count);
        }

        private class Item
        {
            public string Name { get; set; }
            public byte[] Data { get; set; }
        }

        private class NestedArray : AbstractIndexCreationTask<Item>
        {
            public NestedArray()
            {
                Map = docs => from doc in docs
                                           select new Item { Name = doc.Name, Data = doc.Data };
            }
        }
    }
}

