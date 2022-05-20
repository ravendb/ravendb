using System;
using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
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

        DetectIndexType(definition, ref Type, ref SourceType);
    }

    protected IndexInformationHolder([NotNull] Index index)
    {
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        Name = index.Name;
        Definition = index.Definition;
        Configuration = index.Configuration;
        Type = index.Type;
        SourceType = index.SourceType;
    }

    public readonly string Name;

    public readonly IndexDefinitionBaseServerSide Definition;

    public readonly IndexingConfiguration Configuration;

    public readonly IndexType Type;

    public readonly IndexSourceType SourceType;

    public static IndexInformationHolder CreateFor(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration, AbstractStaticIndexBase staticIndex = null)
    {
        if (definition is MapIndexDefinition or MapReduceIndexDefinition)
            return new StaticIndexInformationHolder(definition, configuration, staticIndex);

        Debug.Assert(staticIndex == null, "staticIndex == null");
        return new IndexInformationHolder(definition, configuration);
    }

    public static IndexInformationHolder CreateFor(Index index)
    {
        return index.Type.IsStatic() 
            ? new StaticIndexInformationHolder(index) 
            : new IndexInformationHolder(index);
    }

    private static void DetectIndexType(IndexDefinitionBaseServerSide definition, ref IndexType type, ref IndexSourceType sourceType)
    {
        if (definition is AutoMapIndexDefinition)
        {
            type = IndexType.AutoMap;
            sourceType = IndexSourceType.Documents;
            return;
        }

        if (definition is AutoMapReduceIndexDefinition)
        {
            type = IndexType.AutoMapReduce;
            sourceType = IndexSourceType.Documents;
            return;
        }

        if (definition is FaultyAutoIndexDefinition)
        {
            type = IndexType.Faulty;
            sourceType = IndexSourceType.Documents;
            return;
        }

        if (definition is FaultyIndexDefinition f)
        {
            type = IndexType.Faulty;
            sourceType = f.GetOrCreateIndexDefinitionInternal().SourceType;
            return;
        }

        if (definition is MapReduceIndexDefinition mapReduce)
        {
            type = mapReduce.IndexDefinition.Type;
            sourceType = mapReduce.IndexDefinition.SourceType;
            return;
        }

        if (definition is MapIndexDefinition map)
        {
            type = map.IndexDefinition.Type;
            sourceType = map.IndexDefinition.SourceType;
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(definition));
    }
}

public static class IndexInformationHolderExtensions
{
    public static IndexInformationHolder ToIndexInformationHolder(this Index index) => IndexInformationHolder.CreateFor(index);

    public static BasicIndexInformation ToBasicIndexInformation(this IndexInformationHolder holder) => new BasicIndexInformation
    {
        Name = holder.Name,
        Type = holder.Type,
        SourceType = holder.SourceType,
        Priority = holder.Definition.Priority,
        LockMode = holder.Definition.LockMode,
    };
}
