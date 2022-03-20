using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Esprima;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17423 : RavenTestBase
    {
        public RavenDB_17423(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task FilteredEntryIsReturnedInQuery()
        {
            using var store = GetDocumentStore();
            await store.ExecuteIndexAsync(new SearchIndex());
            var identifiers = new List<string>();
            for (int i = 1; i < 200; i++)
            {
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new Location { Id = $"locations/{i}" });
                identifiers.Add( $"locations/{i}");
                await session.SaveChangesAsync();
                Indexes.WaitForIndexing(store);
    
                var ravenQueryable = session
                    .Query<SearchIndex.Entry, SearchIndex>()
                    .Where(x => x.Identifier.In(identifiers) == false);
                var list = await ravenQueryable.ToListAsync();
                var location = list.FirstOrDefault();
                
                Assert.True(location == null, $"i = {i}");
            }
        }

        [Fact]
        public void CanExcludeOver128ItemsFromMapReduce()
        {
            using var store = GetDocumentStore();
            var index = new SearchIndex();
            store.ExecuteIndex(index);
            var identifiers = new List<string>();
            for (int i = 1; i <= 1000; ++i)
                identifiers.Add( $"locations/{i}"); 
            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var identifier in identifiers)
                    bulkInsert.Store(new Location(){ Id = identifier});
            }

            var excludedList = new List<string>();
            for(int i = 0; i < 257; ++i)
                excludedList.Add(identifiers[i]);
            
            Indexes.WaitForIndexing(store);
            using (var session = store.OpenSession())
            {
                var ravenQueryable = session
                    .Query<SearchIndex.Entry, SearchIndex>()
                    .Where(x => x.Identifier.In(excludedList) == false);
                var list =  ravenQueryable.ToList();
                Assert.Equal(identifiers.Count - 257,list.Count );
                
                
                ravenQueryable = session
                    .Query<SearchIndex.Entry, SearchIndex>()
                    .Where(x => x.Identifier.In(excludedList));
                list =  ravenQueryable.ToList();
                Assert.Equal(excludedList.Count, list.Count);
            }
                
        }

        [Fact]
        public void QueryInAndNotInUsingDynamicQuery()
        {
            using var store = GetDocumentStore();
            var identifiers = new List<string>();
            for (int i = 1; i <= 1000; ++i)
                identifiers.Add( $"locations/{i}");
            
            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var identifier in identifiers)
                    bulkInsert.Store(new Location(){ Id = identifier});
            }

            var testData = new List<string>();
            for(int i = 0; i < 257; ++i)
                testData.Add(identifiers[i]);
            using (var session = store.OpenSession())
            {
                var ravenQueryable = session
                    .Query<Location>()
                    .Where(x => x.Id.In(testData));
                
                var resultList =  ravenQueryable.ToList();
                
                Assert.Equal(testData.Count ,resultList.Count );
                testData.Sort();
                var result = resultList.OrderBy(x => x.Id).Select(x=> x.Id).ToList();
                
                for(int i = 0; i < testData.Count; ++i)
                    Assert.Equal(testData[i], result[i]);
                
                
                
                ravenQueryable = session
                    .Query<Location>()
                    .Where(x => x.Id.In(testData) == false);
                
                resultList =  ravenQueryable.ToList();
                
                Assert.Equal(identifiers.Count - testData.Count ,resultList.Count );
                
                
                result = resultList.OrderBy(x => x.Id).Select(x=> x.Id).ToList();
                var complementTestData = identifiers.Where(x => x.In(testData) == false).ToList();
                complementTestData.Sort();
                
                
                for(int i = 0; i < testData.Count; ++i)
                    Assert.Equal(complementTestData[i], result[i]);
            }
        }

        private class SearchIndex : AbstractMultiMapIndexCreationTask<SearchIndex.Entry>
        {
            public override string IndexName => "Search";

            public SearchIndex()
            {
                AddMap<Location>(locations =>
                    from location in locations
                    select new Entry { Identifier = location.Id, }
                );

                Reduce = entries =>
                    from entry in entries
                    group entry by entry.Identifier
                    into grouping
                    select new Entry { Identifier = grouping.Key };
            }

            public class Entry
            {
                public string Identifier { get; set; }
            }
        }

        private class Location
        {
            public string Id { get; set; }
        }
    }
}
