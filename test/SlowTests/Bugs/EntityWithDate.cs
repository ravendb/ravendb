//-----------------------------------------------------------------------
// <copyright file="EntityWithDate.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Xunit;

namespace SlowTests.Bugs
{
    public class EntityWithDate : RavenTestBase
    {
        [Fact]
        public void CanSerializeAndDeserializeEntityWithDates()
        {
            using(var store = GetDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    session.Store(new Foo{CreatedAt = new DateTime(2010,1,1)});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var load = session.Load<Foo>("foos/1");
                    Assert.Equal(new DateTime(2010,1,1), load.CreatedAt);
                }
            }
        }
        public class Foo
        {
            public string Id { get; set; }
            public DateTime CreatedAt { get; set; }
        }
    }
}
