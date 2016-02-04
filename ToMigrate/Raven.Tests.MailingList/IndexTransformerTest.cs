// -----------------------------------------------------------------------
//  <copyright file="IndexTransformerTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class IndexTransformerTest : RavenTestBase
    {
        [Fact]
        public void CanIndexAndQuery()
        {
            using (var store = NewDocumentStore())
            {
                new SimpleTransformer().Execute(store);
                new SimpleIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestEntity
                    {
                        Name = "Test"
                    });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    // This should fail
                    var result = session.Query<Mapping, SimpleIndex>()
                        .TransformWith<SimpleTransformer, SimpleTransformer.Result>();
                }
            }
        }



        public class TestEntity
        {
            public string Name { get; set; }
        }

        public class SimpleIndex : AbstractIndexCreationTask<TestEntity>
        {
            public SimpleIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Test = true
                              };

                StoreAllFields(FieldStorage.Yes);
            }


        }

        public class Mapping
        {
            public bool Test { get; set; }
        }

        public class SimpleTransformer
            : AbstractTransformerCreationTask<Mapping>
        {
            public SimpleTransformer()
            {
                TransformResults = results =>
                    from result in results
                    select new Result
                    {
                        Test = result.Test
                    };
            }

            public class Result
            {
                public bool Test { get; set; }
            }
        }
    }
}
