//-----------------------------------------------------------------------
// <copyright file="CustomEntityName.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Bugs
{
    public class CustomEntityName : RavenTestBase
    {
        [Fact]
        public void CanCustomizeEntityName()
        {
            using(var store = GetDocumentStore())
            {
                store.Conventions.FindCollectionName = ReflectionUtil.GetFullNameWithoutVersionInformation;

                using(var session = store.OpenSession())
                {
                    session.Store(new Foo{Name = "Ayende"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(Foo));

                    var all = session
                        .Advanced
                        .DocumentQuery<Foo>(collectionName: typeName)
                        .WaitForNonStaleResults(TimeSpan.FromMilliseconds(1000))
                        .ToList();
                    Assert.Equal(1, all.Count);
                }

            }
        }

        public class Foo
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
