using System;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using FastTests.Server.Basic.Entities;
using Xunit;

namespace SlowTests.MailingList
{
    public class OrderByValueTypeCast : RavenNewTestBase
    {
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
