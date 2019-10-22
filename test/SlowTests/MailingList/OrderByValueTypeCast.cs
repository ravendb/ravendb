using System;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class OrderByValueTypeCast : RavenTestBase
    {
        public OrderByValueTypeCast(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    Expression<Func<User, object>> sort = x => x.LastName.Length;
                    s.Query<User>()
                        .OrderBy(sort)
                        .ToList();
                }
            }
        }
    }
}
