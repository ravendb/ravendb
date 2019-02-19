// -----------------------------------------------------------------------
//  <copyright file="StreamingHalfWay.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.MailingList
{
    public class StreamingHalfWay : RavenTestBase
    {
        [Fact(Skip = "Missing feature: /docs/stream")]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateSomeData(store);
                using (var session = store.OpenSession())
                {
                    using (var enumerator = session.Advanced.Stream<dynamic>(startsWith:"foos/"))
                    {
                        enumerator.MoveNext(); // should allow to dispose & move on
                    }
                }
            }
        }

        private class Foo
        {
            public string Id { get; set; }
        }

        private static void CreateSomeData(IDocumentStore store)
        {
            for (int k = 0; k < 10; k++)
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1024; i++)
                    {
                        session.Store(new Foo());
                    }
                    session.SaveChanges();
                }
            }
        }
    }
}
