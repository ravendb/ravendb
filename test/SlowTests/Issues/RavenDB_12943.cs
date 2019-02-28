using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12943 : RavenTestBase
    {
        [Fact]
        public void CanUseQueryMethodConverter()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.RegisterQueryMethodConverter(new MyQueryMethodConverter()) 
            }))
            {
                using (var session = store.OpenSession())
                {
                    var q = session.Query<Company>()
                        .Where(x => x.Address.City == "Warsaw")
                        .ToString();

                    Assert.Equal("from Companies where Test between $p0 and $p1 and Test between $p2 and $p3 and exists(Test2)", q);
                }
            }
        }

        private class MyQueryMethodConverter : QueryMethodConverter
        {
            private bool _isVisiting;

            public override bool Convert<T>(Parameters<T> parameters)
            {
                parameters.DocumentQuery.WhereBetween("Test", 7, 10);

                if (_isVisiting == false)
                {
                    _isVisiting = true;
                    try
                    {
                        parameters.VisitExpression(parameters.Expression);
                    }
                    finally
                    {
                        _isVisiting = false;
                    }
                }
                else
                {
                    parameters.DocumentQuery.WhereExists("Test2");
                }

                return true;
            }
        }
    }
}
