// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4269.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4420 : RavenTest
    {

        [Fact]
        public void CanQueryProperlyWhenSaveEnumAsIntegerIsSetToTrue()
        {
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                // arrange
                store.ExecuteIndex(new MyIndex());

                store.Conventions.SaveEnumsAsIntegers = true;
                InsertTwoDocuments(store);

                var values = new [] { MyEnum.Value1, MyEnum.Value2 };

                WaitForIndexing(store);

                // act
                using (var session = store.OpenSession())
                {

                    var whereInRawQuery = session.Advanced.DocumentQuery<MyDocument, MyIndex>()
                        .WhereIn(x => x.MyProperty, values)
                        .GetIndexQuery(false);

                    var whereInQuery = session
                        .Query<MyDocument, MyIndex>()
                        .Where(x => x.MyProperty.In(values))
                        .ToList();

                    var whereRawQuery = session.Advanced.DocumentQuery<MyDocument, MyIndex>()
                       .WhereEquals(x => x.MyProperty, MyEnum.Value1)
                       .GetIndexQuery(false);

                    var whereQuery = session
                        .Query<MyDocument, MyIndex>()
                        .Where(x => x.MyProperty == MyEnum.Value1)
                        .ToList();

                    // assert
                    Assert.Equal("@in<MyProperty>:(0,1) ", whereInRawQuery.Query);
                    Assert.Equal(2, whereInQuery.Count);

                    Assert.Equal("MyProperty:0", whereRawQuery.Query);
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

        public class MyIndex : AbstractIndexCreationTask<MyDocument>
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

        public enum MyEnum
        {
            Value1,
            Value2
        }

        public class MyDocument
        {
            public MyEnum MyProperty { get; set; }
        }

    }
}