using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Common;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Xunit;


namespace Raven.Tests.Shard.Async
{
    public class ShardedDeleteByIndexAsync : RavenTest
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

        public ShardedDeleteByIndexAsync()
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
        public async Task DeleteByIndexShardedAsync()
        {
            using (var session = shardedDocumentStore.OpenSession())
            {
                var persons = GetNewPersons();
                persons.ForEach((x) =>
                {
                    session.Store(x);
                });

                session.SaveChanges();
            }

            new Person_ByName().Execute(shardedDocumentStore);
            new Person_ByAge().Execute(shardedDocumentStore);

            shardedDocumentStore.WaitForNonStaleIndexesOnAllShards();

            using (var session = shardedDocumentStore.OpenAsyncSession())
            {
                var operation1 = await session.Advanced.DeleteByIndexAsync<Person>("Person/ByName", x => x.Name == "Bob");
                await operation1.WaitForCompletionAsync();

                var operation2 = await session.Advanced.DeleteByIndexAsync<Person, Person_ByAge>(x => x.Age < 35);
                await operation2.WaitForCompletionAsync();

                await session.SaveChangesAsync();
            }
             
            using (var session = shardedDocumentStore.OpenAsyncSession())
            {
                var persons = await session.Advanced.AsyncDocumentQuery<Person>().ToListAsync();

                Assert.Equal(1, persons.Count);
                Assert.Equal("Adi", persons[0].Name);
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
