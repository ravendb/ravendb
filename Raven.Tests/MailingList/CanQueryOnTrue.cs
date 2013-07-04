// -----------------------------------------------------------------------
//  <copyright file="CanQueryOnTrue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.MailingList
{
    public class CanQueryOnTrue : RavenTest
    {
        public class Item
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanQuery()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var e = Assert.Throws<ArgumentException>(
                        () => s.Query<Item>().Where(_ => true).Where(x => x.Name == "oren").ToList());

                    Assert.Equal("Constants expressions such as Where(x => true) are not allowed in the RavenDB queries",e.Message);
                }
            }
        }
    }
}