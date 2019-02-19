using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents;

namespace SlowTests.Issues
{
    public class RavenDB_10995 : RavenTestBase
    {
        class Person
        {
            public string Name { get; set; }
            public Pet Pet { get; set; }
        }

        class Pet
        {
            public int Age { get; set; }
        }

        class PersonVM
        {
            public string Name { get; set; }
            public PetVM Pet { get; set; }
        }

        class PetVM
        {
            public int Age { get; set; }
        }


        class PersonIndex : AbstractIndexCreationTask<Person>
        {
            public PersonIndex()
            {
                Map = persons => from person in persons
                                 select new PersonVM
                                 {
                                     Name = person.Name,
                                     Pet = person.Pet == null ? null : new PetVM
                                     {
                                         Age = person.Pet.Age
                                     }
                                 };
                StoresStrings.Add(Constants.Documents.Indexing.Fields.AllFields, FieldStorage.Yes);
            }
        }

        [Fact]
        public void IndexNullChecks()
        {
            using (var store = GetDocumentStore())
            {
                var john = new Person { Pet = new Pet { Age = 2316 }, Name = "john" };
                var jeff = new Person { Pet = null, Name = "jeff" };

                new PersonIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(john);
                    session.Store(jeff);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<PersonVM>("PersonIndex")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ProjectInto<PersonVM>();
                    Assert.NotEmpty(query1.ToList());
                }
            }
        }
    }
}
