using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Xunit;

namespace SlowTests.MailingList
{
    public class WhereInTests : RavenTestBase
    {
        [Fact]
        public void WhereIn_using_index_notAnalyzed()
        {
            using (IDocumentStore documentStore = GetDocumentStore())
            {
                new PersonsNotAnalyzed().Execute(documentStore);

                string[] names = { "Person One", "PersonTwo" };

                StoreObjects(new List<Person>
                {
                    new Person {Name = names[0]},
                    new Person {Name = names[1]}
                }, documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Person, PersonsNotAnalyzed>().WhereIn(p => p.Name, names, exact: true);
                    Assert.Equal(2, query.ToList().Count());
                }
            }
        }

        [Fact]
        public void SameHash()
        {
            var perFieldAnalyzerComparer = new RavenPerFieldAnalyzerWrapper.PerFieldAnalyzerComparer();
            Assert.Equal(perFieldAnalyzerComparer.GetHashCode("Name"), perFieldAnalyzerComparer.GetHashCode("@in<Name>"));
            Assert.True(perFieldAnalyzerComparer.Equals("Name", "@in<Name>"));
        }

        [Fact]
        public void WhereIn_using_index_analyzed()
        {
            using (IDocumentStore documentStore = GetDocumentStore())
            {
                new PersonsAnalyzed().Execute(documentStore);

                string[] names = { "Person One", "PersonTwo" };

                StoreObjects(new List<Person>
                {
                    new Person {Name = names[0]},
                    new Person {Name = names[1]}
                }, documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Person, PersonsAnalyzed>().Search(p => p.Name, string.Join(" ", names));
                    Assert.Equal(2, query.ToList().Count());
                }
            }
        }

        [Fact]
        public void WhereIn_not_using_index()
        {
            using (IDocumentStore documentStore = GetDocumentStore())
            {

                string[] names = { "Person One", "PersonTwo" };

                StoreObjects(new List<Person>
                {
                    new Person {Name = names[0]},
                    new Person {Name = names[1]}
                }, documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Person>().WhereIn(p => p.Name, names);
                    Assert.Equal(2, query.ToList().Count());
                }
            }
        }

        [Fact]
        public void Where_In_using_query_index_notAnalyzed()
        {
            using (IDocumentStore documentStore = GetDocumentStore())
            {
                new PersonsNotAnalyzed().Execute(documentStore);

                string[] names = { "Person One", "PersonTwo" };

                StoreObjects(new List<Person>
                {
                    new Person {Name = names[0]},
                    new Person {Name = names[1]}
                }, documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var query = session.Query<Person, PersonsNotAnalyzed>().Where(p => p.Name.In(names), exact: true);
                    Assert.Equal(2, query.Count());
                }
            }
        }

        [Fact]
        public void Where_In_using_query_index_analyzed()
        {
            using (IDocumentStore documentStore = GetDocumentStore())
            {
                new PersonsAnalyzed().Execute(documentStore);

                string[] names = { "Person One", "PersonTwo" };

                StoreObjects(new List<Person>
                {
                    new Person {Name = names[0]},
                    new Person {Name = names[1]}
                }, documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var query = session.Query<Person, PersonsAnalyzed>().Search(p => p.Name, string.Join(" ", names));
                    Assert.Equal(2, query.Count());
                }
            }
        }

        [Fact]
        public void Where_In_using_query()
        {
            using (IDocumentStore documentStore = GetDocumentStore())
            {
                string[] names = { "Person One", "PersonTwo" };

                StoreObjects(new List<Person>
                {
                    new Person {Name = names[0]},
                    new Person {Name = names[1]}
                }, documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var query = session.Query<Person>().Where(p => p.Name.In(names));
                    Assert.Equal(2, query.Count());
                }
            }
        }

        private void StoreObjects<T>(IEnumerable<T> objects, IDocumentStore documentStore)
        {
            using (var session = documentStore.OpenSession())
            {
                foreach (var o in objects)
                {
                    session.Store(o);
                }
                session.SaveChanges();
            }
            WaitForIndexing(documentStore);
        }

        private class Person
        {
            public string Name { get; set; }
        }

        private class PersonsNotAnalyzed : AbstractIndexCreationTask<Person>
        {
            public PersonsNotAnalyzed()
            {
                Map = organizations => from o in organizations
                                       select new { o.Name };

                Indexes.Add(x => x.Name, FieldIndexing.Exact);
            }
        }

        private class PersonsAnalyzed : AbstractIndexCreationTask<Person>
        {
            public PersonsAnalyzed()
            {
                Map = organizations => from o in organizations
                                       select new { o.Name };

                Indexes.Add(x => x.Name, FieldIndexing.Search);
            }
        }
    }
}
