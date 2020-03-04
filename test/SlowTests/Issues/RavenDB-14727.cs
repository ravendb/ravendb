using System;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14727 : RavenTestBase
    {
        public RavenDB_14727(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Throw_an_error_when_using_a_certificate_with_an_unsecured_URL()
        {
            const string url = "http://www.ravendb.net";
            using (var store = new DocumentStore
            {
                Database = "HibernatingRhinos",
                Urls = new[] { url },
                Certificate = new X509Certificate2()
            }.Initialize())
            {
                PutDocument();

                // should have the same behaviour
                PutDocument();

                void PutDocument()
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new { Test = "test" }, "test");
                        var exception = Assert.Throws<InvalidOperationException>(() => session.SaveChanges());

                        var errorMessage = $"The url {url} is using HTTP, but a certificate is specified, which require us to use HTTPS";
                        Assert.Equal(errorMessage, exception.Message);
                    }
                }
            }
        }
    }
}
