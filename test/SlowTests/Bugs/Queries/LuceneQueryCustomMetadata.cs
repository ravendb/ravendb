using System.Dynamic;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class LuceneQueryCustomMetadata : RavenTestBase
    {
        public LuceneQueryCustomMetadata(ITestOutputHelper output) : base(output)
        {
        }

        private const string PropertyName = "MyCustomProperty";

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SuccessTest1(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                dynamic expando = new ExpandoObject();

                using (var session = documentStore.OpenSession())
                {
                    session.Store(expando);

                    var metadata = session.Advanced.GetMetadataFor((ExpandoObject)expando);
                    metadata[PropertyName] = true;

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var loaded = session.Load<ExpandoObject>((string)expando.Id);

                    var metadata = session.Advanced.GetMetadataFor(loaded);
                    var token = metadata.GetBoolean(PropertyName);
                    Assert.True(token);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SuccessTest2(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                dynamic expando = new ExpandoObject();

                using (var session = documentStore.OpenSession())
                {
                    session.Store(expando);

                    var metadata = session.Advanced.GetMetadataFor((ExpandoObject)expando);

                    metadata[PropertyName] = "true";

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    dynamic loaded = session.Advanced.DocumentQuery<dynamic>()
                        .WhereEquals("@metadata.@collection",
                                     documentStore.Conventions.GetCollectionName(typeof(ExpandoObject)))
                        .FirstOrDefault();

                    Assert.NotNull(loaded);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void FailureTest(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                dynamic expando = new ExpandoObject();

                using (var session = documentStore.OpenSession())
                {
                    session.Store(expando);

                    var metadata = session.Advanced.GetMetadataFor((ExpandoObject)expando);

                    metadata[PropertyName] = "true";

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    dynamic loaded =
                        session.Advanced.DocumentQuery<dynamic>().WhereEquals("@metadata." + PropertyName, true)
                            .FirstOrDefault();

                    Assert.NotNull(loaded);
                }
            }
        }
    }
}
