// -----------------------------------------------------------------------
//  <copyright file="RavenDB_421.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_421 : RavenTestBase
    {
        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string[] Parents { get; set; }
        }

        private class Family_MultiMapReduce : AbstractMultiMapIndexCreationTask<Family_MultiMapReduce.Result>
        {
            public class Result
            {
                public string PersonId { get; set; }
                public string Name { get; set; }
                public Child[] Children { get; set; }
            }

            public class Child
            {
                public string Id { get; set; }
                public string Name { get; set; }

            }

            public Family_MultiMapReduce()
            {
                AddMap<Person>(people =>
                               from person in people
                               select new
                               {
                                   PersonId = person.Id,
                                   person.Name,
                                   Children = new object[0]
                               });
                AddMap<Person>(people =>
                               from person in people
                               from parent in person.Parents
                               select new
                               {
                                   PersonId = parent,
                                   Name = (string)null,
                                   Children = new[] { new { person.Name, person.Id } }
                               });

                Reduce = results =>
                         from result in results
                         group result by result.PersonId
                             into g
                         select new
                         {
                             PersonId = g.Key,
                             g.FirstOrDefault(x => x.Name != null).Name,
                             Children = g.SelectMany(x => x.Children)
                         };
            }

        }


        private class Family_MultiMap : AbstractMultiMapIndexCreationTask<Family_MultiMap.Result>
        {
            public class Result
            {
                public string PersonId { get; set; }
                public string Name { get; set; }
                public Child[] Children { get; set; }
            }

            public class Child
            {
                public string Id { get; set; }
                public string Name { get; set; }

            }

            public Family_MultiMap()
            {
                AddMap<Person>(people =>
                               from person in people
                               select new
                               {
                                   PersonId = person.Id,
                                   person.Name,
                                   Children = new object[0]
                               });
                AddMap<Person>(people =>
                               from person in people
                               from parent in person.Parents
                               select new
                               {
                                   PersonId = parent,
                                   Name = (string)null,
                                   Children = new[] { new { person.Name, person.Id } }
                               });
            }

        }

        [Fact]
        public void CanExecuteIndexWithoutNRE()
        {
            using (var store = GetDocumentStore())
            {
                var familyMultiMapReduce = new Family_MultiMapReduce();
                familyMultiMapReduce.Execute(store);

                var familyMultiMap = new Family_MultiMap();
                familyMultiMap.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Parent",
                        Parents = new string[0]
                    });
                    session.Store(new Person
                    {
                        Name = "Child",
                        Parents = new[] { "people/1", "people/123" }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Family_MultiMapReduce.Result, Family_MultiMapReduce>()
                        .Where(x => x.PersonId == "people/1")
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Family_MultiMap.Result, Family_MultiMap>()
                        .Where(x => x.PersonId == "people/1")
                        .ProjectInto<Family_MultiMap.Result>()
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                }
            }
        }
    }
}
