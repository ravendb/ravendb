// -----------------------------------------------------------------------
//  <copyright file="FailingIndex.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class FailingAuthIndex : RavenTest
    {
        public class Team
        {
            public string OwnerId;
            public Developer[] Developers;
        }
        public class Developer
        {
            public string UserId;
        }

         [Fact]
         public void ShouldBeAbleToCreate()
         {
             using (var store = NewDocumentStore())
             {
                using (var session = store.OpenSession())
                {
                    var user = new User {Id = "users/1234"};

                    var teams = from team in session.Query<Team>()
                                where team.Developers.Any(d => d.UserId == user.Id)
                                select team;

                    teams.ToArray();

                    teams = from team in session.Query<Team>().Customize(x => x.Include<Team>(t => t.OwnerId))
                                where team.OwnerId == user.Id || team.Developers.Any(d => d.UserId == user.Id)
                                select team;

                    teams.ToArray();
                }
             }
         }
    }
}