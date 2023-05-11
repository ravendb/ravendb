// -----------------------------------------------------------------------
//  <copyright file="RacielRod.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class RacielRod : RavenTestBase
    {
        public RacielRod(ITestOutputHelper output) : base(output)
        {
        }

        private class Activity
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Timer
        {
            public string Id { get; set; }
            public User User { get; set; }
            public Activity Activity { get; set; }
            public DateTimeOffset Start { get; set; }
            public DateTimeOffset? End { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Timer_Test(Options options)
        {
            using (var store = GetDocumentStore(options))
            using (var session = store.OpenSession())
            {
                //insert timers
                session.Store(new Timer
                {
                    Activity = new Activity
                    {
                        Id = "1",
                        Name = "Test1"
                    },
                    Start = DateTimeOffset.Now,
                    User = new User
                    {
                        Id = "users/1",
                        Name = "Test User",
                    }
                });
                session.Store(new Timer
                {
                    Activity = new Activity
                    {
                        Id = "2",
                        Name = "Test2"
                    },
                    Start = DateTimeOffset.Now,
                    User = new User
                    {
                        Id = "users/1",
                        Name = "Test User",
                    }
                });
                session.SaveChanges();

                var runningActivities = session.Query<Timer>()
                    .Where(t => t.End == null && t.User.Id == "users/1")
                    .Select(t => t.Activity.Id)
                    .Customize(t => t.WaitForNonStaleResults())
                    .ToArray();

                Assert.Equal(runningActivities.Length, 2);
                Assert.Equal(runningActivities[0], "1");
            }
        }
    }
}
