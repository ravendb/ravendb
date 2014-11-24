// -----------------------------------------------------------------------
//  <copyright file="ComplexIndexMerge.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class ComplexIndexMerge : RavenTest
    {
        public class Ref
        {
            public Guid Id { get; set; }
        }

        public class Entity
        {
            public Ref EntityARef { get; set; }
            public Ref EntityBRef { get; set; }
        }

        [Fact]
        public void CanQueryOnBothProperties()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.DefaultQueryingConsistency =
                    ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;
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
                    Assert.NotEmpty(session.Query<Entity>().Where(x => x.EntityARef.Id == id).ToList());
                    Assert.NotEmpty(session.Query<Entity>().Where(x => x.EntityBRef.Id == id).ToList());
                    Assert.NotEmpty(session.Query<Entity>().Where(x => x.EntityARef.Id == id).ToList());
                }
            }
        }
    }
}