using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.MailingList
{
    public class Jalchr3 : RavenTestBase
    {
        public Jalchr3(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void Streaming_documents_will_respect_the_sorting_order(RavenTestParameters config)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer => serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                    };
                },
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                }
            }))
            {
                new User_Entity().Execute(store);

                var iteration = 100;
                var list = new List<User>();
                var now = DateTime.Now.AddYears(-50).Date;
                var start = now;
                for (int k = 0; k < iteration; k++)
                {
                    now = now.AddMonths(k);
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 672; i++)
                        {
                            var user = new User();
                            user.Id = Guid.NewGuid().ToString();
                            user.Name = "User" + ("-" + k + "-" + i);
                            user.CreatedDate = now.AddHours(i);
                            list.Add(user);

                            session.Store(user);
                        }
                        session.SaveChanges();
                       
                        Indexes.WaitForIndexing(store);
                    }
                }

                Indexes.WaitForIndexing(store);

                int count;
                // Warm-up
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<User, User_Entity>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats);
                    query = query.Where(x => x.CreatedDate >= start.Date);
                    query = query.Where(x => x.CreatedDate <= DateTime.Now.Date);
                    var result = query.OrderBy(x => x.CreatedDate).Take(25).ToList();

                    Assert.True(result.Count > 0);
                    count = query.Count();
                }

                Indexes.WaitForIndexing(store);

                var orderedList = list.OrderBy(x => x.CreatedDate).ToList();
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<User, User_Entity>()
                        .Statistics(out stats);

                    query = query.Where(x => x.CreatedDate >= start.Date);
                    query = query.Where(x => x.CreatedDate <= DateTime.Now.Date);

                    var streamQuery = query
                        .OrderBy(x => x.CreatedDate)
                        .As<User>();

                    var enumerator = session.Advanced.Stream(streamQuery);
                    var index = 0;
                    while (enumerator.MoveNext())
                    {
                        Assert.True(enumerator.Current.Document.CreatedDate == orderedList[index].CreatedDate,
                            "Failed at: " + index
                            + ", " + enumerator.Current.Document.CreatedDate.ToString("yyyy-MM-dd hh:mm:ss.sssssss")
                            + " != " + orderedList[index].CreatedDate.ToString("yyyy-MM-dd hh:mm:ss.sssssss")
                            );
                        index++;
                    }
                    Assert.Equal(index, count);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
        }

        private class User_Entity : AbstractIndexCreationTask<User>
        {
            public User_Entity()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Id,
                                   user.Name,
                                   user.CreatedDate,
                               };
            }
        }
    }
}
