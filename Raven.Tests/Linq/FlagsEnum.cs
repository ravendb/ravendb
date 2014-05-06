// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
    public class FlagsEnum : RavenTest
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

        protected override void ModifyStore(EmbeddableDocumentStore documentStore)
        {
            documentStore.Conventions.SaveEnumsAsIntegers = true;
        }

        [Fact]
        public void CanQueryUsingEnum()
        {
            using (var store = NewDocumentStore())
            {
                new MyIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var entity = new Entity
                    {
                        Name = "birsey", Status = new[]
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

                    Assert.Empty(store.DocumentDatabase.Statistics.Errors);
                    Assert.NotEmpty(results);
                }
            }
        }
    }
}