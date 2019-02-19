// -----------------------------------------------------------------------
//  <copyright file="Joel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Joel : RavenTestBase
    {
        private class Item
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        private class Index : AbstractIndexCreationTask<Item, Index.Result>
        {
            public class Result
            {
                public object Query { get; set; }
            }

            public Index()
            {
                Map = items =>
                      from item in items
                      select new Result
                      {
                          Query = new object[] { item.Age, item.Name }
                      };
            }
        }

        [Fact]
        public void CanCreateIndexWithExplicitType()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.PrettifyGeneratedLinqExpressions = false;
                }
            }))
            {
                new Index().Execute(store);
                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("Index"));
                Assert.Equal(@"docs.Items.Select(item => new {
    Query = new object[] {
        item.Age,
        item.Name
    }
})".Replace("\r\n", Environment.NewLine), indexDefinition.Maps.First());
            }
        }
    }
}
