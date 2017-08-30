using System;
using FastTests.Server;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.Queries.Dynamic.Map;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;
using Raven.Server.Documents.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using SlowTests.Client.Subscriptions;
using SlowTests.Queries;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Server.Replication;
using SlowTests.Tests.Linq;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore())
            {
                using(store.AggressivelyCache())
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    session.Query<FullTextSearchOnAutoIndex.User>()
                        .Statistics(out stats);
                }
            }
        }
    }
}
