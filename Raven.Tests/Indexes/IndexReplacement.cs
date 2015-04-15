using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
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

	            var indexReplaceDocId = Constants.IndexReplacePrefix + "ReplacementOf/" + new NewIndex().IndexName;

				Assert.True(SpinWait.SpinUntil(() => store.DatabaseCommands.Get(indexReplaceDocId) == null, TimeSpan.FromSeconds(30))); // wait for index replacement

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, OldIndex>()
                                       .Count(x => x.LastName == "Doe");

                    Assert.Equal(0, count);
                }
            }
        }

    }
}
