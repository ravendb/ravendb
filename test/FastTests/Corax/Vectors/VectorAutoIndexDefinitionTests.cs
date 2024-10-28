using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorAutoIndexDefinitionTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private DynamicQueryMapping _sut;
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [InlineData(Constants.VectorSearch.EmbeddingText, VectorEmbeddingType.Text, VectorEmbeddingType.Single, false)]
    [InlineData(Constants.VectorSearch.EmbeddingTextInt8, VectorEmbeddingType.Text, VectorEmbeddingType.Int8, false)]
    [InlineData(Constants.VectorSearch.EmbeddingTextInt1, VectorEmbeddingType.Text, VectorEmbeddingType.Binary, false)]
    [InlineData(null, VectorEmbeddingType.Single, VectorEmbeddingType.Single, false)]
    [InlineData(Constants.VectorSearch.EmbeddingSingleInt8, VectorEmbeddingType.Single, VectorEmbeddingType.Int8, false)]
    [InlineData(Constants.VectorSearch.EmbeddingSingleInt1, VectorEmbeddingType.Single, VectorEmbeddingType.Binary, false)]
    [InlineData(Constants.VectorSearch.EmbeddingInt8, VectorEmbeddingType.Int8, VectorEmbeddingType.Int8, false)]
    [InlineData(Constants.VectorSearch.EmbeddingInt1, VectorEmbeddingType.Binary, VectorEmbeddingType.Binary, false)]
    [InlineData(Constants.VectorSearch.EmbeddingText, VectorEmbeddingType.Text, VectorEmbeddingType.Single, true)]
    [InlineData(Constants.VectorSearch.EmbeddingTextInt8, VectorEmbeddingType.Text, VectorEmbeddingType.Int8, true)]
    [InlineData(Constants.VectorSearch.EmbeddingTextInt1, VectorEmbeddingType.Text, VectorEmbeddingType.Binary, true)]
    [InlineData(null, VectorEmbeddingType.Single, VectorEmbeddingType.Single, true)]
    [InlineData(Constants.VectorSearch.EmbeddingSingleInt8, VectorEmbeddingType.Single, VectorEmbeddingType.Int8, true)]
    [InlineData(Constants.VectorSearch.EmbeddingSingleInt1, VectorEmbeddingType.Single, VectorEmbeddingType.Binary, true)]
    [InlineData(Constants.VectorSearch.EmbeddingInt8, VectorEmbeddingType.Int8, VectorEmbeddingType.Int8, true)]
    [InlineData(Constants.VectorSearch.EmbeddingInt1, VectorEmbeddingType.Binary, VectorEmbeddingType.Binary, true)]
    public void CreateVectorFieldWithMethod(string fieldEmbeddingName, VectorEmbeddingType sourceType, VectorEmbeddingType destinationType, bool aliased)
    {
        var aliasPrefix = (aliased ? "u." : string.Empty);
        var innerNameForQuery = fieldEmbeddingName is null ? $"{aliasPrefix}Name" : $"{fieldEmbeddingName}({aliasPrefix}Name)";
        var nameInIndex = fieldEmbeddingName is null ? "Name" : $"{fieldEmbeddingName}(Name)";
        CreateDynamicMapping($"FROM Users {(aliased ? "as u" : string.Empty)} WHERE vector.search({innerNameForQuery}, 'test')");
        var definition = _sut.CreateAutoIndexDefinition();
        Assert.Equal(1, definition.Collections.Count);
        Assert.Equal("Users", definition.Collections.Single());
        Assert.True(definition.ContainsField($"vector.search({nameInIndex})"));
        Assert.Equal($"Auto/Users/ByVector.search({nameInIndex})", definition.Name);
        Assert.Single(definition.IndexFields);
        var field = definition.IndexFields.Single().Value;
        var vectorOptions = field.Vector;
        Assert.Equal(null, vectorOptions.Dimensions);
        Assert.Equal(sourceType, vectorOptions.SourceEmbeddingType);
        Assert.Equal(destinationType, vectorOptions.DestinationEmbeddingType);
        Assert.Equal(VectorIndexingStrategy.Exact, vectorOptions.IndexingStrategy);
        Assert.IsType<AutoVectorOptions>(vectorOptions);
        Assert.True(vectorOptions is AutoVectorOptions { SourceFieldName: "Name" });
    }


    

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ExtendingIndexWithVectorField()
    {
        _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Placeholder'"));
        var existingDefinition = _sut.CreateAutoIndexDefinition();

        
        (string fieldEmbeddingName, VectorEmbeddingType sourceType, VectorEmbeddingType destinationType)[] fields = {
            (Constants.VectorSearch.EmbeddingText, VectorEmbeddingType.Text, VectorEmbeddingType.Single), 
            (Constants.VectorSearch.EmbeddingTextInt8, VectorEmbeddingType.Text, VectorEmbeddingType.Int8),
            (Constants.VectorSearch.EmbeddingTextInt1, VectorEmbeddingType.Text, VectorEmbeddingType.Binary),
            (null, VectorEmbeddingType.Single, VectorEmbeddingType.Single), 
            (Constants.VectorSearch.EmbeddingSingleInt8, VectorEmbeddingType.Single, VectorEmbeddingType.Int8),
            (Constants.VectorSearch.EmbeddingSingleInt1, VectorEmbeddingType.Single, VectorEmbeddingType.Binary), 
            (Constants.VectorSearch.EmbeddingInt8, VectorEmbeddingType.Int8, VectorEmbeddingType.Int8),
            (Constants.VectorSearch.EmbeddingInt1, VectorEmbeddingType.Binary, VectorEmbeddingType.Binary)
        };

        for (var i = 0; i < fields.Length; ++i)
        {
            var currentField = fields[i];
            var innerName = currentField.fieldEmbeddingName is null ? "Name" : $"{currentField.fieldEmbeddingName}(Name)";
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide($"FROM Users WHERE vector.search({innerName}, 'test')"));
            _sut.ExtendMappingBasedOn(existingDefinition);
            var def = _sut.CreateAutoIndexDefinition();
            Assert.Equal(def.IndexFields.Count, i + 2); // i => i +1 + Name (from above)

            var newField = def.IndexFields[$"vector.search({innerName})"];
            Assert.NotNull(newField);
            Assert.IsType<AutoVectorOptions>(newField.Vector);
            var vectorOptions = (AutoVectorOptions)newField.Vector;
            Assert.Equal(vectorOptions.SourceEmbeddingType, currentField.sourceType);
            Assert.Equal(vectorOptions.DestinationEmbeddingType, currentField.destinationType);
            existingDefinition = def;
        }
    }

    private void CreateDynamicMapping(string query)
    {
        _sut = DynamicQueryMapping.Create(new IndexQueryServerSide(query));
    }
}
