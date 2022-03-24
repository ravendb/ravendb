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
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes;

public class StaticIndexInformationHolder : IndexInformationHolder
{
    public StaticIndexInformationHolder([NotNull] IndexDefinitionBaseServerSide definition, [NotNull] IndexingConfiguration configuration, AbstractStaticIndexBase staticIndex)
        : base(definition, configuration)
    {
        Compiled = staticIndex;
    }

    public StaticIndexInformationHolder([NotNull] Index index)
        : base(index)
    {
        Compiled = index switch
        {
            MapIndex mapIndex => mapIndex._compiled,
            MapReduceIndex mapReduceIndex => mapReduceIndex._compiled,
            MapTimeSeriesIndex mapTimeSeries => mapTimeSeries._compiled,
            MapCountersIndex mapCountersIndex => mapCountersIndex._compiled,
            _ => throw new ArgumentOutOfRangeException(nameof(index))
        };
    }

    public readonly AbstractStaticIndexBase Compiled;
}

public class IndexInformationHolder
{
    protected IndexInformationHolder([NotNull] IndexDefinitionBaseServerSide definition, [NotNull] IndexingConfiguration configuration)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        Name = Definition.Name;
        Type = DetectIndexType(definition);
    }

    protected IndexInformationHolder([NotNull] Index index)
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

    public static IndexInformationHolder CreateFor(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration, AbstractStaticIndexBase staticIndex = null)
    {
        if (definition is MapIndexDefinition or MapReduceIndexDefinition)
            return new StaticIndexInformationHolder(definition, configuration, staticIndex);

        Debug.Assert(staticIndex == null, "staticIndex == null");
        return new IndexInformationHolder(definition, configuration);
    }

    public static IndexInformationHolder CreateFor(Index index)
    {
        switch (index.Type)
        {
            case IndexType.Map:
            case IndexType.MapReduce:
                return new StaticIndexInformationHolder(index);

            default:
                return new IndexInformationHolder(index);
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

        if (definition is MapReduceIndexDefinition mapReduce)
            return mapReduce.IndexDefinition.Type;

        if (definition is MapIndexDefinition map)
            return map.IndexDefinition.Type;

        throw new ArgumentOutOfRangeException(nameof(definition));
    }
}

public static class IndexContextExtensions
{
    public static IndexInformationHolder ToIndexContext(this Index index)
    {
        return IndexInformationHolder.CreateFor(index);
    }
}
