using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_11781 : RavenTestBase
    {
        [Fact]
        public void CanDeployMapReduceIndexWithOutputReduceToCollection()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduce().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Tal" });
                    session.Store(new User { Name = "Idan" });
                    session.Store(new User { Name = "Grisha" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                new MapReduce().Execute(store);
            }
        }

        [Fact]
        public void CanDeployMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduce().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Tal" });
                    session.Store(new User { Name = "Idan" });
                    session.Store(new User { Name = "Grisha" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Throws<IndexInvalidException>(() => new MapReduce2().Execute(store));

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfIndexes);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class MapReduce : AbstractIndexCreationTask<User, MapReduce.Result>
        {
            public override string IndexName => "MapReduce";

            public class Result
            {
                public int Count { get; set; }
                public string Name { get; set; }
            }

            public MapReduce()
            {
                Map = users => from user in users
                               select new Result
                               {
                                   Name = user.Name,
                                   Count = 1
                               };

                Reduce = results => from result in results
                                    group result by result.Name
                    into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };

                OutputReduceToCollection = DocumentConventions.DefaultGetCollectionName(typeof(Result));
            }
        }

        private class MapReduce2 : AbstractIndexCreationTask<User, MapReduce.Result>
        {
            public override string IndexName => "MapReduce";

            public class Result
            {
                public int Count { get; set; }
                public string Name { get; set; }
            }

            public MapReduce2()
            {
                Map = users => from user in users
                               select new Result
                               {
                                   Name = user.Name,
                                   Count = 2
                               };

                Reduce = results => from result in results
                                    group result by result.Name
                    into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };

                OutputReduceToCollection = DocumentConventions.DefaultGetCollectionName(typeof(Result));
            }
        }
    }
}
