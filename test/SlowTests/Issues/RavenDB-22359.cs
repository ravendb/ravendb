using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22359 : RavenTestBase
{
    public RavenDB_22359(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexesWithCorrectAndIncorrectCollectionName()
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from doc in docs.Orders\nselect new {\n    CompanyName = LoadDocument(doc.Company, \"Companies\").Name\n}" },
                Type = IndexType.Map,
                Name = "ShouldNotThrowIndex"
            }));
            
            var ex = Assert.Throws<IndexCompilationException>(() => 
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs.Orders\nselect new {\n    CompanyName = LoadDocument(doc.Company, doc.Company.Split('/')[0]).Name\n}" },
                    Type = IndexType.Map,
                    Name = "ShouldThrowIndex"
                })));

            Assert.Contains("LoadDocument method has to be called with constant value as collection name", ex.Message);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexesWithOldIndexVersion()
    {
        const long oldIndexVersion = IndexDefinitionBaseServerSide.IndexVersion.LoadDocumentWithDynamicCollectionNameShouldThrow - 1;
        
        var incorrectIndexDefinition = new IndexDefinition() { Name = "IncorrectIndex", Maps = {"from doc in docs.Orders\nselect new {\n    CompanyName = LoadDocument(doc.Company, doc.Company.Split('/')[0]).Name\n}"}};
            
        var correctIndexDefinition = new IndexDefinition() { Name = "CorrectIndex", Maps = {"from doc in docs.Orders\nselect new {\n    CompanyName = \"Companies\"\n}"}};
            
        var incorrectIndexBase = IndexCompiler.Compile(incorrectIndexDefinition, oldIndexVersion);
            
        Assert.NotNull(incorrectIndexBase);
            
        var correctIndexBase = IndexCompiler.Compile(correctIndexDefinition, oldIndexVersion);
            
        Assert.NotNull(correctIndexBase);
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexesWithNewIndexVersion()
    {
        const long newIndexVersion = IndexDefinitionBaseServerSide.IndexVersion.LoadDocumentWithDynamicCollectionNameShouldThrow;
        
        var incorrectIndexDefinition = new IndexDefinition() { Name = "IncorrectIndex", Maps = {"from doc in docs.Orders\nselect new {\n    CompanyName = LoadDocument(doc.Company, doc.Company.Split('/')[0]).Name\n}"}};
            
        var correctIndexDefinition = new IndexDefinition() { Name = "CorrectIndex", Maps = {"from doc in docs.Orders\nselect new {\n    CompanyName = \"Companies\"\n}"}};
            
        var correctIndexBase = IndexCompiler.Compile(correctIndexDefinition, newIndexVersion);
            
        Assert.NotNull(correctIndexBase);
        
        var ex = Assert.Throws<IndexCompilationException>(() => IndexCompiler.Compile(incorrectIndexDefinition, newIndexVersion));
        
        Assert.Contains("LoadDocument method has to be called with constant value as collection name", ex.Message);
    }
}
