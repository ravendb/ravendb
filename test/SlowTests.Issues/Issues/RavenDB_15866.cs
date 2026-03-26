using System;
using System.Linq;
using FastTests;
using Xunit;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_15866 : RavenTestBase
    {
        public RavenDB_15866(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Attachments)]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var searchedType = typeof(ViewModel);
                    var result = session.Query<ViewModel>().Where(e => e.Type == searchedType).FirstOrDefault();
                }
            }
        }

        private class ViewModel
        {
            public Type Type { get; set; }
        }
    }
}
