using System;
using System.Collections.Generic;
using System.Reflection;
using FastTests;
using Raven.Server.Config;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[Flags]
public enum RavenSearchEngineMode : byte
{
    Corax = 1 << 1,
    Lucene = 1 << 2,
    All = Corax | Lucene
}

[Flags]
public enum RavenDatabaseMode : byte
{
    Single = 1 << 1,
    Sharded = 1 << 2,
    All = Single | Sharded
}

public class RavenDataAttribute : DataAttribute
{
    public RavenSearchEngineMode SearchEngineMode { get; set; } = RavenSearchEngineMode.Lucene;

    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.Single;

    public object[] Data { get; set; } = null;

    public RavenDataAttribute()
    {
    }

    public RavenDataAttribute(params object[] data)
    {
        Data = data;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        foreach (var (databaseMode, options) in GetOptions(DatabaseMode))
        {
            foreach (var (searchMode, o) in FillOptions(options, SearchEngineMode))
            {
                var length = 1;
                if (Data is { Length: > 0 })
                    length += Data.Length;

                var array = new object[length];
                array[0] = o;

                for (var i = 1; i < array.Length; i++)
                    array[i] = Data[i - 1];

                yield return array;
            }
        }
    }

    internal static IEnumerable<(RavenDatabaseMode Mode, RavenTestBase.Options)> GetOptions(RavenDatabaseMode mode)
    {
        if (mode.HasFlag(RavenDatabaseMode.Single))
            yield return (RavenDatabaseMode.Single, RavenTestBase.Options.ForMode(RavenDatabaseMode.Single));

        if (mode.HasFlag(RavenDatabaseMode.Sharded))
            yield return (RavenDatabaseMode.Sharded, RavenTestBase.Options.ForMode(RavenDatabaseMode.Sharded));
    }

    internal static IEnumerable<(RavenSearchEngineMode SearchMode, RavenTestBase.Options Options)> FillOptions(RavenTestBase.Options options, RavenSearchEngineMode mode)
    {
        if (mode.HasFlag(RavenSearchEngineMode.Corax))
        {
            var coraxOptions = options.Clone();

            coraxOptions.ModifyDatabaseRecord += record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = "Corax";
            };
            coraxOptions.AddToDescription($", {nameof(RavenDataAttribute.SearchEngineMode)} = {nameof(RavenSearchEngineMode.Corax)}");
            yield return (RavenSearchEngineMode.Corax, coraxOptions);
        }

        if (mode.HasFlag(RavenSearchEngineMode.Lucene))
        {
            var luceneOptions = options.Clone();

            luceneOptions.ModifyDatabaseRecord += record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Lucene";
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = "Lucene";
            };
            luceneOptions.AddToDescription($", {nameof(RavenDataAttribute.SearchEngineMode)} = {nameof(RavenSearchEngineMode.Lucene)}");
            yield return (RavenSearchEngineMode.Lucene, luceneOptions);
        }
    }
}
