// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4269.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4420 : RavenTestBase
    {
        public RavenDB_4420(ITestOutputHelper output) : base(output)
        {
        }

        private static void ModifyStore(DocumentStore store)
        {
            store.Conventions.SaveEnumsAsIntegers = true;
        }

        [Fact]
        public void CanQueryProperlyWhenSaveEnumAsIntegerIsSetToTrue()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = ModifyStore
            }))
            {
                // arrange
                store.ExecuteIndex(new MyIndex());

                InsertTwoDocuments(store);

                var values = new[] { MyEnum.Value1, MyEnum.Value2 };

                Indexes.WaitForIndexing(store);

                // act
                using (var session = store.OpenSession())
                {

                    var whereInRawQuery = session.Advanced.DocumentQuery<MyDocument, MyIndex>()
                        .WhereIn(x => x.MyProperty, values)
                        .GetIndexQuery();

                    var whereInQuery = session
                        .Query<MyDocument, MyIndex>()
                        .Where(x => x.MyProperty.In(values))
                        .ToList();

                    var whereRawQuery = session.Advanced.DocumentQuery<MyDocument, MyIndex>()
                       .WhereEquals(x => x.MyProperty, MyEnum.Value1)
                       .GetIndexQuery();

                    var whereQuery = session
                        .Query<MyDocument, MyIndex>()
                        .Where(x => x.MyProperty == MyEnum.Value1)
                        .ToList();

                    // assert
                    Assert.Equal("from index 'MyIndex' where MyProperty in ($p0)", whereInRawQuery.Query);
                    Assert.Contains(MyEnum.Value1, (object[])whereInRawQuery.QueryParameters["p0"]);
                    Assert.Contains(MyEnum.Value2, (object[])whereInRawQuery.QueryParameters["p0"]);
                    Assert.Equal(2, whereInQuery.Count);

                    Assert.Equal("from index 'MyIndex' where MyProperty = $p0", whereRawQuery.Query);
                    Assert.Equal(MyEnum.Value1, whereRawQuery.QueryParameters["p0"]);
                    Assert.Equal(1, whereQuery.Count);
                }
            }
        }

        private static void InsertTwoDocuments(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new MyDocument
                {
                    MyProperty = MyEnum.Value1
                });
                session.Store(new MyDocument
                {
                    MyProperty = MyEnum.Value2
                });

                session.SaveChanges();
            }
        }

        private class MyIndex : AbstractIndexCreationTask<MyDocument>
        {
            public MyIndex()
            {
                Map = document => from s in document
                                  select new
                                  {
                                      s.MyProperty
                                  };
            }
        }

        private enum MyEnum
        {
            Value1,
            Value2
        }

        private class MyDocument
        {
            public MyEnum MyProperty { get; set; }
        }
    }
}
