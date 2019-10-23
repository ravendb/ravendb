//-----------------------------------------------------------------------
// <copyright file="DecimalPrecision.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DecimalPrecision : RavenTestBase
    {
        public DecimalPrecision(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDetectHighPrecision_Decimal()
        {
            using(var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition { Maps = { "from doc in docs select new { doc.M }"},
                    Name = "Precision"}}));

                using(var session = store.OpenSession())
                {
                    session.Store
                        (
                            new Foo
                            {
                                D = 1.33d,
                                F = 1.33f,
                                M = 1.33m
                            }
                        );

                    session.SaveChanges();

                    var count = session.Query<Foo>("Precision")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.M < 1.331m)
                        .Count();

                    Assert.Equal(1, count);

                    count = session.Query<Foo>("Precision")
                        .Customize(x => x.WaitForNonStaleResults(/*TimeSpan.MaxValue*/))
                        .Where(x => x.M > 1.331m)
                        .Count();

                    Assert.Equal(0, count);
                }
            }
        }

        private class Foo
        {
            public decimal M { get; set; }
            public float F { get; set; }
            public double D { get; set; }
        }
    }
}
