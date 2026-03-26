using System;
using System.Net.Sockets;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10965 : RavenTestBase
    {
        public RavenDB_10965(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public Exception E;
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanSerializeException()
        {
            using (var store = GetDocumentStore())
            {
                Exception e = null;
                try
                {
                    throw new SocketException();
                }
                catch (Exception ex)
                {
                    e = ex;
                }
                using (var s = store.OpenSession())
                {
                    s.Store(new Item
                    {
                        E = e
                    });
                    s.SaveChanges();
                }
            }
        }
    }
}
