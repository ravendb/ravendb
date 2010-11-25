using System;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class InvalidIds : LocalClientTest
    {
        [Fact]
        public void DocumentIdCannotStartWithSlash()
        {
            using(var store = NewDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    var invalidOperationException = Assert.Throws<InvalidOperationException>(()=>s.Store(new {Id = "/hello"}));
                    Assert.Equal("Cannot use value '/hello' as a document id because it begins with a '/'", invalidOperationException.Message);
                }
            }
        }
    }
}