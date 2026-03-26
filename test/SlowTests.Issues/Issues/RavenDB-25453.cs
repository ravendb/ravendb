using System.Linq;
using FastTests;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25453 : RavenTestBase
{
    public RavenDB_25453(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void OrderingByUpTo16PropertiesShouldWork(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { 
                    Name1 = "a",  
                    Name2 = "b", 
                    Name3 = "c", 
                    Name4 = "d", 
                    Name5 = "e", 
                    Name6 = "f", 
                    Name7 = "g", 
                    Name8 = "h", 
                    Name9 = "i" ,
                    Name10 = "j",
                    Name11 = "k",
                    Name12 = "l",
                    Name13 = "m",
                    Name14 = "n",
                    Name15 = "o",
                    Name16 = "p",
                    Name17 = "r"
                };
                
                session.Store(dto);
                session.SaveChanges();
                
                var result = session.Query<Dto>()
                    .OrderBy(x => x.Name1)
                    .ThenBy(x => x.Name2)
                    .ThenBy(x => x.Name3)
                    .ThenBy(x => x.Name4)
                    .ThenBy(x => x.Name5)
                    .ThenBy(x => x.Name6)
                    .ThenBy(x => x.Name7)
                    .ThenBy(x => x.Name8)
                    .ThenBy(x => x.Name9)
                    .ThenBy(x => x.Name10)
                    .ThenBy(x => x.Name11)
                    .ThenBy(x => x.Name12)
                    .ThenBy(x => x.Name13)
                    .ThenBy(x => x.Name14)
                    .ThenBy(x => x.Name15)
                    .ThenBy(x => x.Name16)
                    .ToList();
                
                Assert.Single(result);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OrderingByOver16PropertiesShouldThrowOnCorax(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto()
                {
                    Name1 = "a",
                    Name2 = "b",
                    Name3 = "c",
                    Name4 = "d",
                    Name5 = "e",
                    Name6 = "f",
                    Name7 = "g",
                    Name8 = "h",
                    Name9 = "i",
                    Name10 = "j",
                    Name11 = "k",
                    Name12 = "l",
                    Name13 = "m",
                    Name14 = "n",
                    Name15 = "o",
                    Name16 = "p",
                    Name17 = "r"
                };

                session.Store(dto);
                session.SaveChanges();

                var exception = Assert.Throws<RavenException>(() => session.Query<Dto>()
                    .OrderBy(x => x.Name1)
                    .ThenBy(x => x.Name2)
                    .ThenBy(x => x.Name3)
                    .ThenBy(x => x.Name4)
                    .ThenBy(x => x.Name5)
                    .ThenBy(x => x.Name6)
                    .ThenBy(x => x.Name7)
                    .ThenBy(x => x.Name8)
                    .ThenBy(x => x.Name9)
                    .ThenBy(x => x.Name10)
                    .ThenBy(x => x.Name11)
                    .ThenBy(x => x.Name12)
                    .ThenBy(x => x.Name13)
                    .ThenBy(x => x.Name14)
                    .ThenBy(x => x.Name15)
                    .ThenBy(x => x.Name16)
                    .ThenBy(x => x.Name17)
                    .ToList());

                Assert.Contains("Corax does not support ordering by more than 16 properties.", exception.Message);
            }
        }
    }

    private class Dto
    {
        public string Name1 { get; set; }
        public string Name2 { get; set; }
        public string Name3 { get; set; }
        public string Name4 { get; set; }
        public string Name5 { get; set; }
        public string Name6 { get; set; }
        public string Name7 { get; set; }
        public string Name8 { get; set; }
        public string Name9 { get; set; }
        public string Name10 { get; set; }
        public string Name11 { get; set; }
        public string Name12 { get; set; }
        public string Name13 { get; set; }
        public string Name14 { get; set; }
        public string Name15 { get; set; }
        public string Name16 { get; set; }
        public string Name17 { get; set; }
    }
}
