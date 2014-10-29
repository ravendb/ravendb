// -----------------------------------------------------------------------
//  <copyright file="FirstOrDefaultNullableDate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class FirstOrDefaultNullableDate : RavenTest
    {
        public class Item
        {
            public DateTime? At { get; set; }
        } 

        public class Index : AbstractIndexCreationTask<Item, Item>
        {
            public Index()
            {
                Map = items =>
                      from item in items
                      select new {item.At};
                Reduce = items =>
                         from item in items
                         group item by 1
                         into g
                         select new
                         {
                             At = g.Select(x => x.At).FirstOrDefault(x => x != null)
                         };
            }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                new Index().Execute(store);
            }
        }
    }
}