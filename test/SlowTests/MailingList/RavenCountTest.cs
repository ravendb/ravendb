// -----------------------------------------------------------------------
//  <copyright file="RavenCountTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenCountTest : RavenTestBase
    {
        private class TestDocument
        {
            public string Id
            {
                get;
                set;
            }

            public string Prop1
            {
                get;
                set;
            }

            public string Prop2
            {
                get;
                set;
            }
        }

        [Fact]
        public void TestCount()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Prop1 = "abc", Prop2 = "xyz" });
                    session.Store(new TestDocument { Prop1 = "123", Prop2 = "xyz" });
                    session.SaveChanges();
                }

                // ReSharper disable ReplaceWithSingleCallToCount
                using (var session = store.OpenSession())
                {
                    var first = session.Query<TestDocument>()
                                       .Where(d => d.Prop1 == "abc")
                                       .Count(d => d.Prop2 == "xyz");

                    var second = session.Query<TestDocument>()
                                        .Where(d => d.Prop1 == "abc")
                                        .Where(d => d.Prop2 == "xyz")
                                        .Count();

                    Assert.Equal(first, second);
                }
                // ReSharper restore ReplaceWithSingleCallToCount
            }
        }
    }
}
