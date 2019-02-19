// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class FlagsEnum : RavenTestBase
    {
        [Flags]
        private enum CustomEnum
        {
            None,
            One,
            Two,
        }

        private class Entity
        {
            public string Id { set; get; }
            public string Name { set; get; }
            public CustomEnum[] Status { set; get; }
        }

        private class MyIndex : AbstractIndexCreationTask<Entity, MyIndex.Result>
        {
            public class Result
            {
#pragma warning disable 649
                public CustomEnum Status;
#pragma warning restore 649
            }

            public MyIndex()
            {
                Map = entities => from entity in entities
                                  select new
                                  {
                                      Status = entity.Status,
                                  };
            }
        }

        [Fact]
        public void CanQueryUsingEnum()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.SaveEnumsAsIntegers = true;
                }
            }))
            {
                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var entity = new Entity
                    {
                        Name = "birsey",
                        Status = new[]
                        {
                            CustomEnum.One, CustomEnum.Two
                        },
                    };
                    session.Store(entity);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyIndex.Result, MyIndex>()
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .Where(x => x.Status == CustomEnum.Two)
                                         .As<Entity>()
                                         .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.NotEmpty(results);
                }
            }
        }
    }
}
