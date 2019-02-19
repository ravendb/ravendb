// -----------------------------------------------------------------------
//  <copyright file="WithNullableDateTime.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class WithNullableDateTime : RavenTestBase
    {
        [Fact]
        public void CanCreate()
        {
            using (var documentStore = GetDocumentStore())
            {
                new FooIndex().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { NullableDateTime = new DateTime(1989, 3, 15, 7, 28, 1, 1) });
                    session.SaveChanges();

                    Assert.NotNull(session.Query<Foo, FooIndex>()
                               .Customize(c => c.WaitForNonStaleResults())
                               .FirstOrDefault());
                }
            }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map =
                    docs => from doc in docs
                            where doc.NullableDateTime != null
                            select new
                            {
                                doc.NullableDateTime.GetValueOrDefault().Date,
                            };
            }
        }

        private class Foo
        {
            public DateTime? NullableDateTime { get; set; }
        }
    }
}
