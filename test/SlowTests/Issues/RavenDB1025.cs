// -----------------------------------------------------------------------
//  <copyright file="RavenDB1025.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB1025 : RavenTestBase
    {
        public RavenDB1025(ITestOutputHelper output) : base(output)
        {
        }

        private class MyClass
        {
            public int Index { get; set; }
            [JsonProperty(PropertyName = "S")]
            public IList<double> Statistics { get; set; }
        }

        [Fact]
        public void CanSaveAndProject()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new MyClass { Index = 1, Statistics = new List<double> { 1, 3, 4 } });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyClass>()
                      .Customize(x => x.WaitForNonStaleResults())
                      .Select(x => new MyClass
                      {
                          Index = x.Index,
                          Statistics = x.Statistics,
                      }).Single();

                    Assert.NotNull(results.Statistics);
                }

            }
        }
    }
}
