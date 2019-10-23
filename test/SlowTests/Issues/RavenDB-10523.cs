using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10523 : RavenTestBase
    {
        public RavenDB_10523(ITestOutputHelper output) : base(output)
        {
        }

        public class MyIndex : AbstractMultiMapIndexCreationTask<MyIndex.Result>
        {
            public class Result
            {
                public string FirstName;
            }

            public MyIndex()
            {
                AddMap<Employee>(emp => emp.Select(x => new { x.FirstName }));
            }
        }


        [Fact]
        public void LargeNameForMultiMapIndex()
        {
            using (var s = GetDocumentStore())
            {
                new MyIndex().Execute(s);
            }
        }
    }
}
