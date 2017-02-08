using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3987 : RavenTestBase
    {
        private class Person
        {
            public string Name;
            public int Age;
        }

        private class Person_ByName : AbstractIndexCreationTask<Person>
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

        private class Person_ByAge : AbstractIndexCreationTask<Person>
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

        private static Person[] GetNewPersons()
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var persons = GetNewPersons();
                    foreach (var person in persons)
                        await session.StoreAsync(person);

                    await session.SaveChangesAsync();
                }

                new Person_ByName().Execute(store);
                new Person_ByAge().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var operation1 = await session.Advanced.DeleteByIndexAsync<Person>("Person/ByName", x => x.Name == "Bob");
                    await operation1.WaitForCompletionAsync();

                    WaitForIndexing(store);

                    var operation2 = await session.Advanced.DeleteByIndexAsync<Person, Person_ByAge>(x => x.Age < 35);
                    await operation2.WaitForCompletionAsync();

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var persons = GetNewPersons();
                    persons.ForEach(x => session.Store(x));
                    session.SaveChanges();
                }

                new Person_ByName().Execute(store);
                new Person_ByAge().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var operation1 = session.Advanced.DeleteByIndex<Person>("Person/ByName", x => x.Name == "Bob");
                    operation1.WaitForCompletion();

                    WaitForIndexing(store);

                    var operation2 = session.Advanced.DeleteByIndex<Person, Person_ByAge>(x => x.Age < 35);
                    operation2.WaitForCompletion();

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var persons = session.Query<Person>().ToList();

                    Assert.Equal(1, persons.Count);
                    Assert.Equal(persons[0].Name, "Adi");
                }
            }
        }
    }
}

