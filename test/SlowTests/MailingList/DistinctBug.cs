// -----------------------------------------------------------------------
//  <copyright file="DistinctBug.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class DistinctBug : RavenTestBase
    {
        private class TestClass
        {
            public string Id { get; set; }

            public string Value { get; set; }
            public string Value2 { get; set; }
        }

        [Fact]
        public void DistinctByValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestClass() { Value = "test1", Value2 = "" });
                    session.Store(new TestClass() { Value = "test2" });
                    session.Store(new TestClass() { Value = "test3" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var hello = new List<TestClass>();


                    var values = session.Query<TestClass>()
                                        .Select(x => x.Value)
                                        .Distinct()
                                        .ToArray();
                    //this is passing
                    Assert.True(values.Count() == 3);
                    Assert.True(values.Contains("test2"));

                    var values2 = session.Query<TestClass>()
                                        .Select(x => x.Value2)
                                        .Distinct()
                                        .ToArray();

                    //this is not passing
                    Assert.False(values2.Contains("TestClasses-2"));
                    Assert.Equal(2, values2.Count());

                }

            }
        }
    }

}
