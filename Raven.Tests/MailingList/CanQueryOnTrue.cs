// -----------------------------------------------------------------------
//  <copyright file="CanQueryOnTrue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
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
                    s.Query<Item>().Where(x => true).Where(x => x.Name == "oren").ToList();
                }
            }
        }
    }
}