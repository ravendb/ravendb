using System;
using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes;

public class StaticIndexContext : IndexContext
{
    public StaticIndexContext([NotNull] IndexDefinitionBaseServerSide definition, [NotNull] IndexingConfiguration configuration, AbstractStaticIndexBase staticIndex)
        : base(definition, configuration)
    {
        Compiled = staticIndex;
    }

    public StaticIndexContext([NotNull] Index index)
        : base(index)
    {
        Compiled = index switch
        {
            MapIndex mapIndex => mapIndex._compiled,
            MapReduceIndex mapReduceIndex => mapReduceIndex._compiled,
            _ => throw new ArgumentOutOfRangeException(nameof(index))
        };
    }

    public readonly AbstractStaticIndexBase Compiled;
}

public class IndexContext
{
    protected IndexContext([NotNull] IndexDefinitionBaseServerSide definition, [NotNull] IndexingConfiguration configuration)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        Name = Definition.Name;
        Type = DetectIndexType(definition);
    }

    protected IndexContext([NotNull] Index index)
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

    public static IndexContext CreateFor(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration, AbstractStaticIndexBase staticIndex = null)
    {
        if (definition is MapIndexDefinition or MapReduceIndexDefinition)
            return new StaticIndexContext(definition, configuration, staticIndex);

        Debug.Assert(staticIndex == null, "staticIndex == null");
        return new IndexContext(definition, configuration);
    }

    public static IndexContext CreateFor(Index index)
    {
        switch (index.Type)
        {
            case IndexType.Map:
            case IndexType.MapReduce:
                return new StaticIndexContext(index);

            default:
                return new IndexContext(index);
        }
    }

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
    public static IndexContext ToIndexContext(this Index index)
    {
        return IndexContext.CreateFor(index);
    }
}
