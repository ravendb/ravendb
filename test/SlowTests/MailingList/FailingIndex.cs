// -----------------------------------------------------------------------
//  <copyright file="FailingIndex.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.MailingList
{
    public class FailingAuthIndex : RavenTestBase
    {
        private class Team
        {
#pragma warning disable 649
            public string OwnerId;
            public Developer[] Developers;
#pragma warning restore 649
        }

        private class Developer
        {
#pragma warning disable 649
            public string UserId;
#pragma warning restore 649
        }

        private class User
        {
            public string Id { get; set; }
        }

        [Fact]
        public void ShouldBeAbleToCreate()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1234" };

                    var teams = from team in session.Query<Team>()
                                where team.Developers.Any(d => d.UserId == user.Id)
                                select team;

                    teams.ToArray();

                    teams = from team in session.Query<Team>().Include(x => x.OwnerId)
                            where team.OwnerId == user.Id || team.Developers.Any(d => d.UserId == user.Id)
                            select team;

                    teams.ToArray();
                }
            }
        }
    }
}
