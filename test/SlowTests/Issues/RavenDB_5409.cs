using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5409 : RavenTestBase
    {
        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name
                               };

                Analyzers.Add(x => x.Name, "NotExistingAnalyzer");
            }
        }

        [Fact]
        public void AnalyzerErrorsShouldMarkIndexAsErroredImmediately()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 1; i++)
                    {
                        session.Store(new User());
                    }

                    session.SaveChanges();
                }

                new Users_ByName().Execute(store);

                var result = SpinWait.SpinUntil(() => store.DatabaseCommands.GetStatistics().Indexes[0].State == IndexState.Error, TimeSpan.FromSeconds(5));

                Assert.True(result, "Index did not become errored.");
            }
        }
    }
}