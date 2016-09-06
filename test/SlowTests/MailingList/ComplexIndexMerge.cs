// -----------------------------------------------------------------------
//  <copyright file="ComplexIndexMerge.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class ComplexIndexMerge : RavenTestBase
    {
        private class Ref
        {
            public Guid Id { get; set; }
        }

        private class Entity
        {
            public Ref EntityARef { get; set; }
            public Ref EntityBRef { get; set; }
        }

        [Fact]
        public void CanQueryOnBothProperties()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Entity
                    {
                        EntityARef = new Ref(),
                        EntityBRef = new Ref()
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var id = Guid.Empty;
                    Assert.NotEmpty(session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.EntityARef.Id == id).ToList());
                    Assert.NotEmpty(session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.EntityBRef.Id == id).ToList());
                    Assert.NotEmpty(session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.EntityARef.Id == id).ToList());
                }
            }
        }
    }
}
