using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes;

public class IndexContext
{
    public IndexContext([NotNull] IndexDefinitionBaseServerSide definition, [NotNull] IndexingConfiguration configuration)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        Name = Definition.Name;
        Type = DetectIndexType(definition);
    }

    public IndexContext([NotNull] Index index)
    {
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        Name = index.Name;
        Definition = index.Definition;
        Configuration = index.Configuration;
        Type = index.Type;
    }

    public readonly string Name;

    public readonly IndexDefinitionBaseServerSide Definition;

    public readonly IndexingConfiguration Configuration;

    public readonly IndexType Type;

    private static IndexType DetectIndexType(IndexDefinitionBaseServerSide definition)
    {
        if (definition is AutoMapIndexDefinition)
            return IndexType.AutoMap;

        if (definition is AutoMapReduceIndexDefinition)
            return IndexType.AutoMapReduce;

        if (definition is FaultyIndexDefinition or FaultyAutoIndexDefinition)
            return IndexType.Faulty;

        if (definition is MapReduceIndexDefinition)
            return IndexType.MapReduce;

        if (definition is MapIndexDefinition)
            return IndexType.Map;

        throw new ArgumentOutOfRangeException(nameof(definition));
    }
}

public static class IndexContextExtensions
{
    public static IndexContext ToIndexContext(this Index index) => new IndexContext(index);
}
