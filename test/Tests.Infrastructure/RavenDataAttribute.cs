using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

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

public class RavenDataWithRandomSeedAttribute(params object[] data) : RavenDataAttribute(data)
{
    public override async ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var baseResult = await base.GetData(testMethod, disposalTracker);
        var result = new List<ITheoryDataRow>();
        foreach (var current in baseResult)
        {
            var currentData = current.GetData();
            var parametersWithRandomSeed = new object[currentData.Length + 1];
            Array.Copy(currentData, parametersWithRandomSeed, currentData.Length);
            parametersWithRandomSeed[^1] = Random.Shared.Next();
            result.Add(new TheoryDataRow(parametersWithRandomSeed));
        }

        return result;
    }
}

public class RavenDataAttribute : RavenDataAttributeBase
{
    public RavenSearchEngineMode SearchEngineMode { get; set; } = RavenSearchEngineMode.Lucene;

    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.Single;

    public object[] Data { get; set; } = null;

    public RavenDataAttribute()
    {
    }

    public RavenDataAttribute(params object[] data)
    {
        Data = data ?? [null];
    }

    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var result = new List<ITheoryDataRow>();
        foreach (var (databaseMode, options) in GetOptions(DatabaseMode))
        {
            foreach (var (_, o) in FillOptions(options, SearchEngineMode))
            {
                using (SkipIfNeeded(databaseMode))
                {
                    var length = 1;
                    if (Data is { Length: > 0 })
                        length += Data.Length;

                    var array = new object[length];
                    array[0] = o;

                    for (var i = 1; i < array.Length; i++)
                        array[i] = Data[i - 1];

                    result.Add(new TheoryDataRow(array));
                }
            }
        }
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
    }

    internal static IEnumerable<(RavenDatabaseMode Mode, RavenTestBase.Options)> GetOptions(RavenDatabaseMode mode)
    {
        if (mode.HasFlag(RavenDatabaseMode.Single))
            yield return (RavenDatabaseMode.Single, RavenTestBase.Options.ForMode(RavenDatabaseMode.Single));

        if (mode.HasFlag(RavenDatabaseMode.Sharded))
            yield return (RavenDatabaseMode.Sharded, RavenTestBase.Options.ForMode(RavenDatabaseMode.Sharded));
    }

    internal static IEnumerable<(RavenSearchEngineMode SearchEngineMode, RavenTestBase.Options Options)> FillOptions(RavenTestBase.Options options, RavenSearchEngineMode mode)
    {
        if (mode.HasFlag(RavenSearchEngineMode.Corax))
        {
            var coraxOptions = options.Clone();
            coraxOptions.SearchEngineMode = RavenSearchEngineMode.Corax;
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
            luceneOptions.SearchEngineMode = RavenSearchEngineMode.Lucene;

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
