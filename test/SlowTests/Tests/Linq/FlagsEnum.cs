// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client;

namespace SlowTests.Tests.Linq
{
    public class FlagsEnum : RavenTestBase
    {
        [Flags]
        public enum CustomEnum
        {
            None,
            One,
            Two,
        }

        public class Entity
        {
            public string Id { set; get; }
            public string Name { set; get; }
            public CustomEnum[] Status { set; get; }
        }

        public class MyIndex : AbstractIndexCreationTask<Entity, MyIndex.Result>
        {
            public class Result
            {
                public CustomEnum Status;
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
        public async Task CanQueryUsingEnum()
        {
            using (var store = await GetDocumentStore())
            {
                store.Conventions.SaveEnumsAsIntegers = true;

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

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyIndex.Result, MyIndex>()
                                         .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                                         .Where(x => x.Status == CustomEnum.Two)
                                         .As<Entity>()
                                         .ToList();

                    var errors = store.DatabaseCommands.GetIndexErrors().SelectMany(x => x.Errors);

                    Assert.Empty(errors);
                    Assert.NotEmpty(results);
                }
            }
        }
    }
}
