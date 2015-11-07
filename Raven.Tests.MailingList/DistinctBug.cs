// -----------------------------------------------------------------------
//  <copyright file="DistinctBug.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class DistinctBug : RavenTest
    {

        public class TestClass
        {
            public string Id { get; set; }

            public string Value { get; set; }
            public string Value2 { get; set; }
        }

        [Fact]
        public void DistinctByValue()
        {
            var str = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                Conventions = { IdentityPartsSeparator = "-" },
                UseEmbeddedHttpServer = true,
                Configuration =
                {
                    Port = 8079
                }

            };
            str.Configuration.Storage.Voron.AllowOn32Bits = true;
            using (var store = str
                 .Initialize())
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
