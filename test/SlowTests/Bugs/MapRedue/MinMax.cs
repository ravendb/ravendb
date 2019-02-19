using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs.MapRedue
{
    public class MinMax : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string UserName { get; set; }
        }

        private class LogInAction
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public bool WasSuccessful { get; set; }

            private DateTime? _loggedInAt;
            public DateTime? LoggedInAt
            {
                get
                {
                    if (_loggedInAt == null && LoggedInAtWithOffset.HasValue)
                    {
                        _loggedInAt = LoggedInAtWithOffset.Value.DateTime;
                    }
                    return _loggedInAt;
                }
                set { _loggedInAt = value; }
            }

            public DateTimeOffset? LoggedInAtWithOffset { get; set; }
        }

        [Fact]
        public void CanUseMaxOnNullableDateTimeOffset()
        {
            using (var store = GetDocumentStore())
            {              
                using (var session = store.OpenSession())
                {
                    var ayende = new User { UserName = "Ayende" };
                    session.Store(ayende);

                    session.Store(new LogInAction
                    {
                        UserId = ayende.Id,
                        LoggedInAtWithOffset = DateTimeOffset.UtcNow.AddDays(-4),
                        WasSuccessful = false,
                    });
                    session.Store(new LogInAction
                    {
                        UserId = ayende.Id,
                        LoggedInAtWithOffset = DateTimeOffset.UtcNow.AddDays(-3),
                        WasSuccessful = false,
                    });
                    session.Store(new LogInAction
                    {
                        UserId = ayende.Id,
                        LoggedInAtWithOffset = DateTimeOffset.UtcNow.AddDays(-2),
                        WasSuccessful = true,
                    });

                    session.SaveChanges();
                }

                new Users_LastLoggedInAt().Execute((IDocumentStore)store);

                using (var session = store.OpenSession())
                {
                    var max = session.Query<Users_LastLoggedInAt.Result, Users_LastLoggedInAt>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToList();

                    var db = GetDocumentDatabaseInstanceFor(store).Result;
                    var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                    Assert.Equal(0, errorsCount);

                    Assert.NotEmpty(max);
                }
            }
        }

        private class Users_LastLoggedInAt : AbstractMultiMapIndexCreationTask<Users_LastLoggedInAt.Result>
        {
            public class Result
            {
                public string UserName { get; set; }
                public DateTime? LoggedInAt { get; set; }
                public DateTimeOffset? LoggedInAtWithOffset { get; set; }
            }

            public Users_LastLoggedInAt()
            {
                AddMap<User>(users => users.Select(user => new Result
                {
                    UserName = user.UserName,
                    LoggedInAt = (DateTime?)null,
                    LoggedInAtWithOffset = (DateTimeOffset?)null,
                }));

                AddMap<LogInAction>(actions => actions.Select(action => new Result
                {
                    UserName = (string)null,
                    LoggedInAt = action.LoggedInAt,
                    LoggedInAtWithOffset = action.LoggedInAtWithOffset
                }));

                Reduce = results => from result in results
                                    group result by result.UserName
                                    into g
                                    select new Result
                                    {
                                        UserName = g.Key,
                                        LoggedInAt = g.Max(x => x.LoggedInAt),
                                        LoggedInAtWithOffset = g.Max(x => x.LoggedInAtWithOffset),
                                    };
            }
        }
    }
}
