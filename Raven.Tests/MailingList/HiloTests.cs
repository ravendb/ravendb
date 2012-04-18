// -----------------------------------------------------------------------
//  <copyright file="HiloTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
    public class HiloTests : RavenTest
    {
        [Fact]
        public void HiloCannotGoDown()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put(
                        "Raven/Hilo/Users", null,
                        new RavenJObject
                        {
                            {"Max", 32}
                        },
                        new RavenJObject());

                var hiLoKeyGenerator = new HiLoKeyGenerator(store.DatabaseCommands, "Users", 32);

                var ids = new HashSet<long> {hiLoKeyGenerator.NextId()};

                store.DatabaseCommands.Put(
                      "Raven/Hilo/Users", null,
                      new RavenJObject
                        {
                            {"Max", 12}
                        },
                      new RavenJObject());


                for (int i = 0; i < 128; i++)
                {
                    if(i == 30)
                    {
                        
                    }
                    Assert.True(ids.Add(hiLoKeyGenerator.NextId()), "Failed at " + i);
                }

                var list = ids.GroupBy(x => x).Select(g => new
                {
                    g.Key, Count = g.Count()
                }).Where(x => x.Count > 1).ToList();

                Assert.Empty(list);
            }
        }

        [Fact]
        public void ShouldResolveConflictWithHighestNumber()
        {
            using(var server = GetNewServer())
            using(var store = new DocumentStore
                                  {
                                      Url = "http://localhost:8079"
                                  }.Initialize())
            {
                server.Database.TransactionalStorage.Batch(accessor =>
                {
                    accessor.Documents.AddDocument(
                        "Raven/Hilo/Users/Conflict/1", null,
                        new RavenJObject
                        {
                            {"Max", 32}
                        },
                        new RavenJObject());

                    accessor.Documents.AddDocument(
                        "Raven/Hilo/Users/Conflict/2", null,
                        new RavenJObject
                        {
                            {"Max", 64}
                        },
                        new RavenJObject());

                    accessor.Documents.AddDocument("Raven/Hilo/Users", null,
                                                   new RavenJObject
                                                   {
                                                       {
                                                           "Conflicts",
                                                           new RavenJArray()
                                                           {
                                                               "Raven/Hilo/Users/Conflict/1",
                                                               "Raven/Hilo/Users/Conflict/2"
                                                           }
                                                           }
                                                   },
                                                   new RavenJObject
                                                   {
                                                       {
                                                           "@Http-Status-Code"
                                                           ,
                                                           409
                                                           },
                                                       {
                                                           "@Http-Status-Description"
                                                           , "Conflicted doc"
                                                           }
                                                   });
                });

                var hiLoKeyGenerator = new HiLoKeyGenerator(store.DatabaseCommands, "Users", 32);
                var nextId = hiLoKeyGenerator.NextId();
                Assert.Equal(65, nextId);
            }
        }


    }
}