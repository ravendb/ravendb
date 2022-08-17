using System;
using System.Dynamic;
using FastTests;
using Raven.Client;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19146 : RavenTestBase
    {
        public RavenDB_19146(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Use_StartsWith_In_Empty_Collection()
        {
            using (var store = GetDocumentStore())
            {
                string id = "test";
                using (var session = store.OpenSession())
                {
                    var expando = new ExpandoObject();
                    session.Store(expando, id);

                    var metadata = session.Advanced.GetMetadataFor((ExpandoObject)expando);
                    metadata[Constants.Documents.Metadata.Collection] = null;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced
                        .RawQuery<dynamic>($"from @empty where startsWith(id(), '{id[0]}')")
                        .ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }
    }
}
