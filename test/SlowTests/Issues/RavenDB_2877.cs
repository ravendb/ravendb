// -----------------------------------------------------------------------
//  <copyright file="RavenDB2854.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2877 : RavenTestBase
    {
        public RavenDB_2877(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Name { get; set; }
            public List<string> Offices { get; set; }
        }

        private class Office
        {
            public string FacilityName { get; set; }
            public int OfficeNumber { get; set; }
        }

        private class PersonsIndex : AbstractIndexCreationTask<Person>
        {
            public class Result
            {
                public string Name { get; set; }
            }
            public PersonsIndex()
            {
                Map = results => from result in results
                                 select new Result
                                 {
                                     Name = result.Name
                                 };
            }
        }


        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanHandleHandleLongUrl(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new PersonsIndex().Execute(store);

                //store.Conventions.MaxLengthOfQueryUsingGetUrl = 32;
                var offices = Enumerable.Range(1, 20).Select(x => new Office { FacilityName = "Main Offices", OfficeNumber = x });

                using (var s = store.OpenSession())
                {
                    foreach (var office in offices)
                    {
                        s.Store(office, "office/" + office.OfficeNumber);
                    }

                    var person = new Person
                    {
                        Name = "John",
                        Offices = offices.Select(x => "office/" + x.OfficeNumber).ToList()
                    };

                    s.Store(person, "person/1");

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Person, PersonsIndex>().Customize(x => x.WaitForNonStaleResults());
                    results.ToList();
                    results.FirstOrDefault(x => x.Name == "John");
                    results.Include<Person>(x => x.Offices).ToList();

                    results = s.Query<Person, PersonsIndex>();
                    s.Advanced.Stream<Person>(results).MoveNext();
                    s.Advanced.Stream<Person>(results.Where(x => x.Name == "John")).MoveNext();
                }

                using (var s = store.OpenAsyncSession())
                {
                    var results = s.Query<Person>();
                    await results.ToListAsync();
                    await results.FirstOrDefaultAsync(x => x.Name == "John");
                    await results.Include<Person>(x => x.Offices).ToListAsync();

                    results = s.Query<Person, PersonsIndex>();
                    await s.Advanced.StreamAsync<Person>(results).ConfigureAwait(false);
                    await s.Advanced.StreamAsync<Person>(results.Where(x => x.Name == "John")).ConfigureAwait(false);

                }
            }
        }
    }
}
