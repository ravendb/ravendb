using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22753 : RavenTestBase
{
    public RavenDB_22753(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Indexes)]
    public void MustSendSearchEngineType()
    {
        using var store = GetDocumentStore();

        IndexCreation.CreateIndexes(new []
        {
            new Users_ByName()
        }, store);

        IndexDefinition[] indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 1));

        Assert.Contains(Constants.Configuration.Indexes.IndexingStaticSearchEngineType, (IDictionary<string, string>) indexDefinitions[0].Configuration);
        Assert.Equal("Corax", indexDefinitions[0].Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType]);
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
}
