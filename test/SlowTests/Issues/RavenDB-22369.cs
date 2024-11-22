using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22369 : RavenTestBase
{
    public RavenDB_22369(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void CannotCreateSpatialFieldWithoutIndexingJs() => CannotCreateSpatialFieldWithoutIndexingBase<JsSpatialInvalidIndex>();

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void CannotCreateSpatialFieldWithoutIndexingCSharp() => CannotCreateSpatialFieldWithoutIndexingBase<CsharpSpatialInvalidIndex>();
    
    private void CannotCreateSpatialFieldWithoutIndexingBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new SpatialDto(10, 10));
        session.SaveChanges();
        var index = new TIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store, allowErrors: true);
        WaitForValue(ErrorExist, true);
        var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {index.IndexName}));
        Assert.Contains($"Your spatial field 'Location' has 'Indexing' set to 'No'. Spatial fields cannot be stored, so this field is useless because it cannot be searched or retrieved.",
            indexErrors[0].Errors[0].ToString());

        bool ErrorExist()
        {
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }));
            return errors != null && errors.Length > 0 && errors[0].Errors != null && errors[0].Errors.Length > 0;
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void CoraxCanOnlyStoreFieldCsharpIndex() => CoraxCanOnlyStoreFieldBase<CsharpMapIndex>();

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void CoraxCanOnlyStoreFieldJsIndex() => CoraxCanOnlyStoreFieldBase<JsMapIndex>();
    
    private void CoraxCanOnlyStoreFieldBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new Dto("Test", 12.5D));
        session.SaveChanges();
        var index = new TIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Dto.Name), fromValue: null));
        Assert.Equal(0, terms.Length);

        terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Dto.Numerical), fromValue: null));
        Assert.Equal(0, terms.Length);

        var resultTextual = session.Advanced.DocumentQuery<Dto>(index.IndexName).SelectFields<string>(nameof(Dto.Name)).First();
        Assert.Equal("Test", resultTextual);
        
        var resultNumerical = session.Advanced.DocumentQuery<Dto>(index.IndexName).SelectFields<double>(nameof(Dto.Numerical)).First();
        Assert.Equal(12.5D,resultNumerical);
    }

    private record Dto(string Name, double Numerical, string Id = null);

    private record SpatialDto(double Lat, double Lon, string Id = null);

    private class JsMapIndex : AbstractJavaScriptIndexCreationTask
    {
        public JsMapIndex()
        {
            Maps = new HashSet<string>(){@"
map(""Dtos"", (dto) => {
    return {
        Name: dto.Name,
        Numerical: dto.Numerical,
    };
})"};
            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                { "Name", new IndexFieldOptions() { Indexing = FieldIndexing.No, Storage = FieldStorage.Yes } },
                { "Numerical", new IndexFieldOptions() { Indexing = FieldIndexing.No, Storage = FieldStorage.Yes } }
            };
        }
    }

    private class JsSpatialInvalidIndex : AbstractJavaScriptIndexCreationTask
    {
        public JsSpatialInvalidIndex()
        {
            Maps = new HashSet<string>(){@"
map(""SpatialDtos"", (dto) => {
    return {
        Location: createSpatialField(dto.Lat, dto.Lon)
    };
})"};
            Fields = new Dictionary<string, IndexFieldOptions>() { { "Location", new IndexFieldOptions() { Indexing = FieldIndexing.No, Storage = FieldStorage.Yes} } };
        }
    }
    
    private class CsharpSpatialInvalidIndex : AbstractIndexCreationTask<SpatialDto>
    {
        public CsharpSpatialInvalidIndex()
        {
            Map = dtos => from dto in dtos
                select new { Location = CreateSpatialField(dto.Lat, dto.Lon) };

            Index("Location", FieldIndexing.No);
            Store("Location", FieldStorage.Yes);
        }
    }
    
    private class CsharpMapIndex : AbstractIndexCreationTask<Dto>
    {
        public CsharpMapIndex()
        {
            Map = dtos => from dto in dtos
                select new { Name = dto.Name, dto.Numerical };

            Store(x => x.Name, FieldStorage.Yes);
            Store(x => x.Numerical, FieldStorage.Yes);
            Index(x => x.Name, FieldIndexing.No);
            Index(x => x.Numerical, FieldIndexing.No);
        }
    }
}
