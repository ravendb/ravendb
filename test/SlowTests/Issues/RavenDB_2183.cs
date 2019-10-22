// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2183.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2183 : RavenTestBase
    {
        public RavenDB_2183(ITestOutputHelper output) : base(output)
        {
        }

        private class Address
        {
            public string Street { get; set; }
        }

        [Fact]
        public void DynamicListShouldContainTakeMethod()
        {
            var list =
                new DynamicArray(
                    new List<Address>
                    {
                        new Address { Street = "Street1" },
                        new Address { Street = "Street2" },
                        new Address { Street = "Street3" }
                    });

            Assert.Equal(2, list.Take(2).Count());
        }
    }
}
