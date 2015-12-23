using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Indexes;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics;
using Raven.Tests.Common;
using Xunit;
using Raven.Tests.Helpers;

namespace Raven.Tests.Issues
{
    public class RavenDB_3987 : RavenTest
    {
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
        public async Task DeleteByIndexAsync()
        {
            using (var server = GetNewServer())
            {
                using (var session = server.DocumentStore.OpenSession())
                {
                    var persons = GetNewPersons();
                    persons.ForEach((x) =>
                    {
                        session.Store(x);
                    });

                    session.SaveChanges();
                }

                new Person_ByName().Execute(server.DocumentStore);
                new Person_ByAge().Execute(server.DocumentStore);

                WaitForIndexing(server.DocumentStore);

                using (var session = server.DocumentStore.OpenAsyncSession())
                {
                    var operation1 = await session.Advanced.DeleteByIndexAsync<Person>("Person/ByName", x => x.Name == "Bob");
                    await operation1.WaitForCompletionAsync();

                    var operation2 = await session.Advanced.DeleteByIndexAsync<Person, Person_ByAge>(x => x.Age < 35);
                    await operation2.WaitForCompletionAsync();

                    await session.SaveChangesAsync();
                }

                using (var session = server.DocumentStore.OpenAsyncSession())
                {
                    var persons = await session.Advanced.AsyncDocumentQuery<Person>().ToListAsync();

                    Assert.Equal(1, persons.Count);
                    Assert.Equal("Adi", persons[0].Name);
                }
            }
        }

        [Fact]
        public void DeleteByIndex()
        {
            using (var server = GetNewServer())
            {
                using (var session = server.DocumentStore.OpenSession())
                {
                    var persons = GetNewPersons();
                    persons.ForEach(x => session.Store(x));
                    session.SaveChanges();
                }

                new Person_ByName().Execute(server.DocumentStore);
                new Person_ByAge().Execute(server.DocumentStore);

                WaitForIndexing(server.DocumentStore);

                using (var session = server.DocumentStore.OpenSession())
                {
                    var operation1 = session.Advanced.DeleteByIndex<Person>("Person/ByName", x => x.Name == "Bob");
                    operation1.WaitForCompletion();

                    var operation2 = session.Advanced.DeleteByIndex<Person, Person_ByAge>(x => x.Age < 35);
                    operation2.WaitForCompletion();

                    session.SaveChanges();
                }

                using (var session = server.DocumentStore.OpenSession())
                {
                    var persons = session.Query<Person>().ToList();

                    Assert.Equal(1, persons.Count);
                    Assert.Equal(persons[0].Name, "Adi");
                }
            }
        }
    }
}

