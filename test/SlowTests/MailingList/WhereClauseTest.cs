// -----------------------------------------------------------------------
//  <copyright file="WhereClauseTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.MailingList
{
    public class WhereClauseTest : RavenTestBase
    {
        [Fact]
        public async Task ATest()
        {
            using (var ds = await GetDocumentStore())
            {
                using (IDocumentSession session = ds.OpenSession())
                {
                    session.Store(new TestEntity(int.MaxValue));
                    session.SaveChanges();
                }


                using (IDocumentSession qSession = ds.OpenSession())
                {
                    var entities = qSession.Query<TestEntity>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Where(x => x.IntType > 0)
                        .ToList();

                    Assert.True(entities.Count > 0);
                }
            }
        }

        public class TestEntity
        {
            public TestEntity(int intValue)
            {
                IntType = intValue;
            }

            public string Id { get; set; }
            public int IntType { get; set; }
        }
    }
}
