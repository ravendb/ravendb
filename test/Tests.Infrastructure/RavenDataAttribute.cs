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
    //Corax = 1 << 1,
    Lucene = 1 << 2,
    All = /*Corax | */Lucene
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

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        foreach (var options in GetOptions(DatabaseMode))
        {
            foreach (var o in FillOptions(options, SearchEngineMode))
                yield return new object[] { o };
        }
    }

    private static IEnumerable<RavenTestBase.Options> GetOptions(RavenDatabaseMode mode)
    {
        if (mode.HasFlag(RavenDatabaseMode.Single))
            yield return new RavenTestBase.Options();

        if (mode.HasFlag(RavenDatabaseMode.Sharded))
            yield return RavenTestBase.Options.WithSharding();
    }

    private static IEnumerable<RavenTestBase.Options> FillOptions(RavenTestBase.Options options, RavenSearchEngineMode mode)
    {
        //if (mode.HasFlag(RavenSearchEngineMode.Corax))
        //{
        //    var coraxOptions = options.Clone();

        //    coraxOptions.ModifyDatabaseRecord += record =>
        //    {
        //        record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "Corax";
        //        record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer)] = "Corax";
        //    };

        //    yield return coraxOptions;
        //}

        if (mode.HasFlag(RavenSearchEngineMode.Lucene))
        {
            var luceneOptions = options.Clone();

            //luceneOptions.ModifyDatabaseRecord += record =>
            //{
            //    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "Lucene";
            //    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer)] = "Lucene";
            //};

            yield return luceneOptions;
        }
    }
}
