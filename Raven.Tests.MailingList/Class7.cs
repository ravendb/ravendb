using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class Class7 : RavenTestBase
    {
        [Fact]
        public void ThrowsOnUnindexedSorts()
        {
            using (var store = NewDocumentStore())
            {
                new PersonIndex().Execute(store);

                Person personA;
                Person personB;
                using (var session = store.OpenSession())
                {
                    personA = new Person();
                    personA.Name = "A";
                    personA.Surname = "A";
                    session.Store(personA);

                    personB = new Person();
                    personB.Name = "B";
                    personB.Surname = "B";
                    session.Store(personB);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Person, PersonIndex>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .OrderByDescending(x=>x.Surname)
                        .ToList();
					Assert.True(results[0] == personB);
                    Assert.True(results[1] == personA);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.LuceneQuery<Person, PersonIndex>()
                        .WaitForNonStaleResultsAsOfNow()
                        .OrderByDescending(x => x.Surname)
                        .ToList();
					Assert.True(results[0] == personB);
					Assert.True(results[1] == personA);
                }
            }
        }




		public class Person
		{
			public string Name { get; set; }
			public string Surname { get; set; }
		}

		public class PersonIndex : AbstractIndexCreationTask<Person>
		{
			public PersonIndex()
			{
				Map = persons => from person in persons
								 select new
								 {
									 person.Name,
								 };
			}
		}
    }

}
