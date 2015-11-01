using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Indexes
{
    public class IndexReplacementTest : RavenTest
    {

        private class Person
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        private class OldIndex : AbstractIndexCreationTask<Person>
        {
            public override string IndexName
            {
                get { return "The/Index"; }
            }

            public OldIndex()
            {
                Map = persons => from person in persons select new { person.FirstName };
            }
        }

        private class NewIndex : AbstractIndexCreationTask<Person>
        {
            public override string IndexName
            {
                get { return "The/Index"; }
            }

            public NewIndex()
            {
                Map = persons => from person in persons select new { person.FirstName, person.LastName };
            }
        }

        private class NewIndex2 : AbstractIndexCreationTask<Person>
        {
            public NewIndex2()
            {
                Map = persons => from person in persons select new { person.FirstName, person.LastName, person.Id };
            }
        }

        [Fact]
        public void ReplaceAfterNonStale()
        {
            using (var store = NewDocumentStore())
            {
                IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(OldIndex))), store);

                WaitForIndexing(store);

                var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, OldIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

                IndexCreation.SideBySideCreateIndexes(new CompositionContainer(new TypeCatalog(typeof(NewIndex))), store);
               
                WaitForIndexing(store);

                var indexReplaceDocId = Constants.IndexReplacePrefix + Constants.SideBySideIndexNamePrefix + new NewIndex().IndexName;

                Assert.True(SpinWait.SpinUntil(() => store.DatabaseCommands.Get(indexReplaceDocId) == null, TimeSpan.FromSeconds(30))); // wait for index replacement

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, OldIndex>()
                                       .Count(x => x.LastName == "Doe");

                    Assert.Equal(0, count);
                }
            }
        }

        [Fact]
        public void ShouldNotReplaceIndexIfSameDefinition()
        {
            using (var store = NewDocumentStore())
            {
                IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(OldIndex))), store);
                WaitForIndexing(store);

                var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, OldIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                var error = "The field 'LastName' is not indexed, cannot query on fields that are not indexed";
                Assert.Contains(error, e.InnerException.Message);

                IndexCreation.SideBySideCreateIndexes(new CompositionContainer(new TypeCatalog(typeof(OldIndex))), store);
                WaitForIndexing(store);

                Assert.Equal(2, store.DatabaseCommands.GetStatistics().CountOfIndexes);

                e = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, OldIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);
            }
        }

        [Fact]
        public void SideBySideIndexCreated()
        {
            using (var store = NewDocumentStore())
            {
                IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof (OldIndex))), store);

                WaitForIndexing(store);

                var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, OldIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

                var mre = new ManualResetEventSlim(false);
                var documentDatabase = store.ServerIfEmbedded.Server.GetDatabaseInternal(store.DefaultDatabase).Result;
                documentDatabase.IndexReplacer.IndexReplaced += s =>
                {
                    mre.Set();
                };
                IndexCreation.SideBySideCreateIndexes(new CompositionContainer(new TypeCatalog(typeof (NewIndex))), store);
                WaitForUserToContinueTheTest(store);

                Assert.True(mre.Wait(10000));
                Assert.Equal(2, store.DatabaseCommands.GetStatistics().CountOfIndexes);
            }
        }

    }
}
