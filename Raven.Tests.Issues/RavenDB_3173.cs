// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3173.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3173 : RavenTestBase
    {

        public class Item { }

        [Fact]
        public void CannotSaveInvalidHeaders()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var entity = new Item();
                    s.Store(entity);
                    s.Advanced.GetMetadataFor(entity)["you@there"] = true;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<Item>(1);
                }
            }
        }
    }
}