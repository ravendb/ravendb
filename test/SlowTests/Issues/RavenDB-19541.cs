using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19541 : RavenTestBase
{
    public RavenDB_19541(ITestOutputHelper output) : base(output)
    {
    }

    private void AssertResult(Action queryToString)
    {
        var exception = Assert.ThrowsAny<InvalidOperationException>(queryToString);
        Assert.True(exception.Message.Contains("Projection is already done. You should not project your result twice."));
    }
    
    [Fact]
    public void CanProjectOnlyOneFieldAfterProjectInto() // Hits: ExpressionType.MemberAccess
    {
        using var store = GetDocumentStoreWithDocuments();
        using var session = OpenSessionAndGetProjectIntoQuery(store, out var projectIntoQuery);

        var selectSingle = projectIntoQuery.Select(i => i.FirstName);

        AssertResult(() => selectSingle.ToString());
    }

    [Fact]
    public void CanProjectIntoAnonymousAfterProjectInfo() // Hits:  ExpressionType.New
    {
        using var store = GetDocumentStoreWithDocuments();
        using var session = OpenSessionAndGetProjectIntoQuery(store, out var projectIntoQuery);
        var selectAnonymous = projectIntoQuery.Select(i => new {i.FirstName, i.FavoriteFood});
        AssertResult(() => selectAnonymous.ToString());
    }

    private class MemberInitProjection
    {
        public string FirstName { get; set; }

        public string FavoriteFood { get; set; }
    }

    [Fact]
    public void CanProjectIntoKnownMemberAfterProjectInfo() // Hits:  ExpressionType.MemberInit
    {
        using var store = GetDocumentStoreWithDocuments();
        using var session = OpenSessionAndGetProjectIntoQuery(store, out var projectIntoQuery);
        var memberInitQuery = projectIntoQuery.Select(i => new MemberInitProjection() {FirstName = i.FirstName, FavoriteFood = i.FavoriteFood});

        AssertResult(() => memberInitQuery.ToString());
    }

    [Fact]
    public void CanProjectIntoCallAfterProjectInfo() // Hits:  ExpressionType.Call
    {
        using var store = GetDocumentStoreWithDocuments();
        using var session = OpenSessionAndGetProjectIntoQuery(store, out var projectIntoQuery);
        var selectAnonymous = projectIntoQuery.Select(into => into.Props["nested"]);
        
        AssertResult(() => selectAnonymous.ToString());
    }

    [Fact]
    public void DocumentQueryProjectionWithMultipleSelects()
    {
        using var store = GetDocumentStoreWithDocuments();

        using (var session = store.OpenSession())
        {
            var projectIntoQuery = session.Advanced.DocumentQuery<FullData, CapsLockIndex>().WhereEquals(i => i.FirstName, "maciej").SelectFields<ProjectionInto>();
            Assert.Equal("from index 'CapsLockIndex' where FirstName = $p0 select FirstName, LastNameName, Age, FavoriteFood, Props", projectIntoQuery.ToString());
            var dqProjectionIntoAnother = projectIntoQuery.SelectFields<MemberInitProjection>();
            Assert.Equal("from index 'CapsLockIndex' where FirstName = $p0 select FirstName, FavoriteFood", dqProjectionIntoAnother.ToString());
            Assert.Equal("MACIEJ", dqProjectionIntoAnother.Single().FirstName);
            Assert.Equal("Zylc", dqProjectionIntoAnother.Single().FavoriteFood);        }
    }
    
    [Fact]
    public async Task AsyncDocumentQueryProjectionWithMultipleSelects()
    {
        using var store = GetDocumentStoreWithDocuments();

        using (var session = store.OpenAsyncSession())
        {
            var projectIntoQuery = session.Advanced.AsyncDocumentQuery<FullData, CapsLockIndex>().WhereEquals(i => i.FirstName, "maciej").SelectFields<ProjectionInto>();
            Assert.Equal("from index 'CapsLockIndex' where FirstName = $p0 select FirstName, LastNameName, Age, FavoriteFood, Props", projectIntoQuery.ToString());
            var dqProjectionIntoAnother = projectIntoQuery.SelectFields<MemberInitProjection>();
            Assert.Equal("from index 'CapsLockIndex' where FirstName = $p0 select FirstName, FavoriteFood", dqProjectionIntoAnother.ToString());
            Assert.Equal("MACIEJ", (await dqProjectionIntoAnother.SingleAsync()).FirstName);
            Assert.Equal("Zylc", (await dqProjectionIntoAnother.SingleAsync()).FavoriteFood);
        }
    }
    
    private IDocumentSession OpenSessionAndGetProjectIntoQuery(IDocumentStore store, out IRavenQueryable<ProjectionInto> projectIntoQuery)
    {
        var session = store.OpenSession();
        projectIntoQuery = Queryable.Where(session.Query<FullData, CapsLockIndex>(), i => i.FirstName == "maciej").ProjectInto<ProjectionInto>();
        Assert.Equal("from index 'CapsLockIndex' where FirstName = $p0 select FirstName, LastNameName, Age, FavoriteFood, Props", projectIntoQuery.ToString());
        return session;
    }

    private IDocumentStore GetDocumentStoreWithDocuments()
    {
        var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new FullData("Maciej", "John", 123, "Zylc", "Torun", "Szeroka", new Dictionary<string, int> {{"nested", 1}}));
            session.Store(new FullData("John", "Book", 99, "Burger", "New York", "French street", default));
            session.SaveChanges();
        }

        new CapsLockIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class CapsLockIndex : AbstractIndexCreationTask<FullData>
    {
        public CapsLockIndex()
        {
            Map = data => from i in data
                let x = i.Props == null ? -1 : i.Props["nested"]
                select new
                {
                    FirstName = i.FirstName.ToUpper(CultureInfo.InvariantCulture),
                    Age = i.Age % 97,
                    City = i.City.ToLowerInvariant(),
                    Props_nested = x
                };

            Store(i => i.FirstName, FieldStorage.Yes);
            Store(i => i.Age, FieldStorage.Yes);
            Store(i => i.City, FieldStorage.Yes);
            Store("Props_nested", FieldStorage.Yes);
        }
    }

    private class FullData
    {
        public FullData()
        {
        }
        
        public FullData(string firstName, string lastName, int age, string favoriteFood, string city, string street, Dictionary<string, int> props)
        {
            FirstName = firstName;
            LastName = lastName;
            Age = age;
            FavoriteFood = favoriteFood;
            City = city;
            Street = street;
            Props = props;
        }
        
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }
        public string FavoriteFood { get; set; }
        public string City { get; set; }
        public string Street { get; set; }
        public Dictionary<string, int> Props { get; set; }
    }

    private class ProjectionInto
    {
        public string FirstName { get; set; }
        public string LastNameName { get; set; }
        public int Age { get; set; }
        public string FavoriteFood { get; set; }

        public Dictionary<string, int> Props { get; set; }
    }
}
