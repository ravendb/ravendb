using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Subscriptions;
using SmartComponents.LocalEmbeddings;
using Sparrow;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23473(ITestOutputHelper output) : RavenTestBase(output)
{
    public static LocalEmbedder LocalEmbedder = new LocalEmbedder();


    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void IndexVectorWhenPreviousElementsAreNullWithoutExplicitVectorFieldConfiguration()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto() { };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        new DtoIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var dto = session.Load<Dto>(id);
            dto.Vector = [.1f, .2f, .3f];
            session.Store(dto);
            session.SaveChanges();
        }

        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
    }

    private class Dto
    {
        public string Id { get; set; }
        public float[] Vector { get; set; }
    }

    private class DtoIndex : AbstractIndexCreationTask<Dto>
    {
        public DtoIndex()
        {
            Map = dtos => from dto in dtos select new { Vector = CreateVector(dto.Vector) };
        }
    }
}
