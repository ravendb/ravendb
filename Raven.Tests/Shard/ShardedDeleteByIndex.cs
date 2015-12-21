// -----------------------------------------------------------------------
//  <copyright file="SimpleSharding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Bugs;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Shard
{
    public class ShardedDeleteByIndex : RavenTest
    {
        private new readonly RavenDbServer[] servers;
        private readonly ShardedDocumentStore shardedDocumentStore;

        public class Person
        {
            public string Name;
            public int Age;
        }

        public class Person_ByName : AbstractIndexCreationTask<Person>
        {
            public Person_ByName()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     Name = person.Name
                                 };
            }
        }

        public class Person_ByAge : AbstractIndexCreationTask<Person>
        {
            public Person_ByAge()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     Age = person.Age
                                 };
            }
        }

        public ShardedDeleteByIndex()
        {
            servers = new[]
            {
                GetNewServer(8079),
                GetNewServer(8078),
                GetNewServer(8077),
            };

            shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
            {
                {"1", CreateDocumentStore(8079)},
                {"2", CreateDocumentStore(8078)},
                {"3", CreateDocumentStore(8077)}
            }));
            shardedDocumentStore.Initialize();
        }


        public static IDocumentStore CreateDocumentStore(int port)
        {
            return new DocumentStore
            {
                Url = string.Format("http://localhost:{0}/", port),
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately
                }
            };
        }

        public static Person[] GetNewPersons()
        {
            return new Person[]
            {
                new Person()
                {
                    Name = "Bob",
                    Age = 40
                },
                new Person()
                {
                    Name = "Bob",
                    Age = 25
                },
                new Person()
                {
                    Name = "Bob",
                    Age = 42
                },
                new Person()
                {
                    Name = "Bob",
                    Age = 40
                },
                new Person()
                {
                    Name = "Bobina",
                    Age = 30
                },
                 new Person()
                {
                    Name = "Bob",
                    Age = 43
                },
                new Person()
                {
                    Name = "Adi",
                    Age = 40
                },
                new Person()
                {
                    Name = "Adina",
                    Age = 20
                }
            };
        }

        [Fact]
        public void DeleteByIndexSharded()
        {
            using (var session = shardedDocumentStore.OpenSession())
            {
                var persons = GetNewPersons();
                persons.ForEach(x => session.Store(x));
                session.SaveChanges();
            }

            new Person_ByName().Execute(shardedDocumentStore);
            new Person_ByAge().Execute(shardedDocumentStore);

            Assert.True(shardedDocumentStore.WaitForNonStaleIndexesOnAllShards());

            using (var session = shardedDocumentStore.OpenSession())
            {
                var operation1 = session.Advanced.DeleteByIndex<Person>("Person/ByName", x => x.Name == "Bob");
                operation1.WaitForCompletion();

                var operation2 = session.Advanced.DeleteByIndex<Person, Person_ByAge>(x => x.Age < 35);
                operation2.WaitForCompletion();

                session.SaveChanges();
            }

            using (var session = shardedDocumentStore.OpenSession())
            {
                var persons = session.Query<Person>().ToList();

                Assert.Equal(persons.Count, 1);
                Assert.Equal(persons[0].Name, "Adi");
            }
        }

        public override void Dispose()
        {
            shardedDocumentStore.Dispose();
            foreach (var server in servers)
            {
                server.Dispose();
            }
            base.Dispose();
        }
    }
}
