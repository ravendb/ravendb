using System;
using FastTests;
using Raven.Client;
using Sparrow.Server.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23642(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void UnboundedBetweenTimeQueries(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        var upTo = new DateTime(2021, 1, 1).ToString("O");

        var user2021 =
            new User { StartDate = new DateTime(2021, 1, 1), 
                EndDate = new DateTime(2021, 1, 31),
                Textual = upTo
            };
        var user2022 =
            new User { StartDate = new DateTime(2022, 1, 1), EndDate = new DateTime(2022, 1, 31), Textual = "Maciej"};
        var emptyUser = new User();

        session.Store(user2021);
        session.Store(user2022);
        session.Store(emptyUser);
        session.SaveChanges();

        var results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereBetween(x => x.StartDate, new DateTime(2022, 1, 1), null)
            .ToList();
        Assert.Equal(2, results.Count);
        Assert.Equal(user2022.Id, results[0].Id);
        Assert.Equal(emptyUser.Id, results[1].Id);

        results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereBetween(x => x.StartDate, null, new DateTime(2021, 12, 31))
            .ToList();
        Assert.Equal(1, results.Count);
        Assert.Equal(user2021.Id, results[0].Id);


        results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereBetween(x => x.StartDate, null, null)
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal(user2021.Id, results[0].Id);
        Assert.Equal(user2022.Id, results[1].Id);
        Assert.Equal(emptyUser.Id, results[2].Id);


        results = session.Advanced
            .RawQuery<User>($"from Users where Textual between '1' and '{upTo}'")
            .WaitForNonStaleResults()
            .ToList();
        Assert.Equal(1, results.Count);
        Assert.Equal(user2021.Id, results[0].Id);

        results = session.Advanced
            .RawQuery<User>($"from Users where Textual between 'm' and 'Maciej'")
            .WaitForNonStaleResults()
            .ToList();
        
        Assert.Equal(1, results.Count);
        Assert.Equal(user2022.Id, results[0].Id);

    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CoraxUnboundedTextualBetweenQueries(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        
        var u0 = new User() { Textual = "&" };
        var u1 = new User() { Textual = "&a" };
        var u2 = new User() { Textual = "aa" };
        var u3 = new User() { Textual = "o" };
        var u4 = new User() { Textual = null };
        session.Store(u0);
        session.Store(u1);
        session.Store(u2);
        session.Store(u3);
        session.Store(u4);
        session.SaveChanges();


        var results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereBetween(x => x.Textual, null, "aa")
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(u0.Id, results[0].Id);
        Assert.Equal(u1.Id, results[1].Id);
        Assert.Equal(u2.Id, results[2].Id);
        
        
        results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereBetween(x => x.Textual, "&a", null)
            .ToList();
        
        Assert.Equal(4, results.Count);
        Assert.Equal(u1.Id, results[0].Id);
        Assert.Equal(u2.Id, results[1].Id);
        Assert.Equal(u3.Id, results[2].Id);
        Assert.Equal(u4.Id, results[3].Id);
        
        results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereBetween(x => x.Textual, null, null)
            .ToList();
        
        Assert.Equal(5, results.Count);
        Assert.Equal(u0.Id, results[0].Id);
        Assert.Equal(u1.Id, results[1].Id);
        Assert.Equal(u2.Id, results[2].Id);
        Assert.Equal(u3.Id, results[3].Id);
        Assert.Equal(u4.Id, results[4].Id);
        
        results = session.Advanced
            .DocumentQuery<User>()
            .WaitForNonStaleResults()
            .WhereExists(x => x.Textual)
            .AndAlso()
            .WhereBetween(x => x.Textual, null, null)
            .ToList();
        
        Assert.Equal(5, results.Count);
        Assert.Equal(u0.Id, results[0].Id);
        Assert.Equal(u1.Id, results[1].Id);
        Assert.Equal(u2.Id, results[2].Id);
        Assert.Equal(u3.Id, results[3].Id);
        Assert.Equal(u4.Id, results[4].Id);
    }

    private class User
    {
        public string Id { get; set; }
        public string Textual { get; set; }

        public float? Number { get; set; }

        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
    }
}
