using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;

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
	                var e = Assert.Throws<InvalidOperationException>(() =>
	                {
						var results = session.Query<Person, PersonIndex>()
							.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
							.OrderByDescending(x => x.Surname)
							.ToList();
	                });

					Assert.Equal("Query failed. See inner exception for details.", e.Message);
					Assert.Contains("The field 'Surname' is not indexed, cannot sort on fields that are not indexed", e.InnerException.Message);
                }

                using (var session = store.OpenSession())
                {
					var e = Assert.Throws<InvalidOperationException>(() =>
					{
						var results = session.Advanced.DocumentQuery<Person, PersonIndex>()
						   .WaitForNonStaleResultsAsOfNow()
						   .OrderByDescending(x => x.Surname)
						   .ToList();
					});

					Assert.Equal("Query failed. See inner exception for details.", e.Message);
					Assert.Contains("The field 'Surname' is not indexed, cannot sort on fields that are not indexed", e.InnerException.Message);
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
