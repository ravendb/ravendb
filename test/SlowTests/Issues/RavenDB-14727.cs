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
        public void Throw_an_error_when_using_incorrect_url_with_a_certificate()
        {
            const string url1 = "http://www.ravendb.net";

            var store = new DocumentStore
            {
                Database = "HibernatingRhinos",
                Urls = new[] { url1 },
#pragma warning disable SYSLIB0026 // Type or member is obsolete
                Certificate = new X509Certificate2()
#pragma warning restore SYSLIB0026 // Type or member is obsolete
            };

            Exception exception = Assert.Throws<InvalidOperationException>(() => store.Initialize());
            var errorMessage = $"The url {url1} is using HTTP, but a certificate is specified, which require us to use HTTPS";
            Assert.Equal(errorMessage, exception.Message);

            const string url2 = "www.ravendb.net";
            exception = Assert.Throws<ArgumentException>(() => store.Urls = new[] { url2 });
            errorMessage = $"'{url2}' is not a valid url";
            Assert.Equal(errorMessage, exception.Message);

            exception = Assert.Throws<ArgumentNullException>(() => store.Urls = new[] { (string)null });
            errorMessage = "Urls cannot contain null (Parameter 'value')";
            Assert.Equal(errorMessage, exception.Message);

            store.Urls = new[] { url1, "https://www.ravendb.net" };
            store.Certificate = null;
            exception = Assert.Throws<InvalidOperationException>(() => store.Initialize());
            errorMessage = $"The url {url1} is using HTTP, but other urls are using HTTPS, and mixing of HTTP and HTTPS is not allowed";
            Assert.Equal(errorMessage, exception.Message);
        }
    }
}
