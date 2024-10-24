using System.Linq;
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
    [InlineData("embedding.text", EmbeddingType.Text, EmbeddingType.Single, false)]
    [InlineData("embedding.text_i8", EmbeddingType.Text, EmbeddingType.Int8, false)]
    [InlineData("embedding.text_i1", EmbeddingType.Text, EmbeddingType.Binary, false)]
    [InlineData(null, EmbeddingType.Single, EmbeddingType.Single, false)]
    [InlineData("embedding.f32_i8", EmbeddingType.Single, EmbeddingType.Int8, false)]
    [InlineData("embedding.f32_i1", EmbeddingType.Single, EmbeddingType.Binary, false)]
    [InlineData("embedding.i8", EmbeddingType.Int8, EmbeddingType.Int8, false)]
    [InlineData("embedding.i1", EmbeddingType.Binary, EmbeddingType.Binary, false)]
    [InlineData("embedding.text", EmbeddingType.Text, EmbeddingType.Single, true)]
    [InlineData("embedding.text_i8", EmbeddingType.Text, EmbeddingType.Int8, true)]
    [InlineData("embedding.text_i1", EmbeddingType.Text, EmbeddingType.Binary, true)]
    [InlineData(null, EmbeddingType.Single, EmbeddingType.Single, true)]
    [InlineData("embedding.f32_i8", EmbeddingType.Single, EmbeddingType.Int8, true)]
    [InlineData("embedding.f32_i1", EmbeddingType.Single, EmbeddingType.Binary, true)]
    [InlineData("embedding.i8", EmbeddingType.Int8, EmbeddingType.Int8, true)]
    [InlineData("embedding.i1", EmbeddingType.Binary, EmbeddingType.Binary, true)]
    public void CreateVectorFieldWithMethod(string fieldEmbeddingName, EmbeddingType sourceType, EmbeddingType destinationType, bool aliased)
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

        
        (string fieldEmbeddingName, EmbeddingType sourceType, EmbeddingType destinationType)[] fields = {
            ("embedding.text", EmbeddingType.Text, EmbeddingType.Single), 
            ("embedding.text_i8", EmbeddingType.Text, EmbeddingType.Int8),
            ("embedding.text_i1", EmbeddingType.Text, EmbeddingType.Binary),
            (null, EmbeddingType.Single, EmbeddingType.Single), 
            ("embedding.f32_i8", EmbeddingType.Single, EmbeddingType.Int8),
            ("embedding.f32_i1", EmbeddingType.Single, EmbeddingType.Binary), 
            ("embedding.i8", EmbeddingType.Int8, EmbeddingType.Int8),
            ("embedding.i1", EmbeddingType.Binary, EmbeddingType.Binary)
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
