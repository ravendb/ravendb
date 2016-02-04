// -----------------------------------------------------------------------
//  <copyright file="NickDoubleValues.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class NickDoubleValues : RavenTest
    {
        public class Item
        {
            public double Lat;
        }

        [Fact]
        public void Local()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Item { Lat = -73.6247069 });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    var load = s.Load<Item>(1);
                    Assert.Equal(-73.6247069, load.Lat);
                }
            }
        }

        [Fact]
        public void Remote()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Item { Lat = -73.6247069 });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    var load = s.Load<Item>(1);
                    Assert.Equal(-73.6247069, load.Lat);
                }
            }
        }
    }
}
