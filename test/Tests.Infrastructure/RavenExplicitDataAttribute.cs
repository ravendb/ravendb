using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure;

public class RavenExplicitDataAttribute : RavenDataAttributeBase
{
    public RavenSearchEngineMode SearchEngineMode { get; set; }

    public RavenDatabaseMode DatabaseMode { get; set; }

    public object[] Data { get; set; } = null;

    public RavenExplicitDataAttribute(
        RavenDatabaseMode databaseMode = RavenDatabaseMode.Single,
        RavenSearchEngineMode searchEngine = RavenSearchEngineMode.Lucene
    )
    {
        DatabaseMode = databaseMode;
        SearchEngineMode = searchEngine;
    }

    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var result = new List<ITheoryDataRow>();
        foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
        {
            foreach (var (searchMode, o) in RavenDataAttribute.FillOptions(options, SearchEngineMode))
            {
                using (SkipIfNeeded(databaseMode))
                {
                    var length = 1;
                    if (Data is { Length: > 0 })
                        length += Data.Length;

                    var array = new object[length];

                    array[0] = new RavenTestParameters { SearchEngine = searchMode, DatabaseMode = databaseMode, Options = o };

                    for (var i = 1; i < array.Length; i++)
                        array[i] = Data[i - 1];

                    result.Add(new TheoryDataRow(array));
                }
            }
        }
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
    }
}

public class RavenTestParameters
{
    public RavenSearchEngineMode SearchEngine;

    public RavenDatabaseMode DatabaseMode;

    public RavenTestBase.Options Options;
    
    public override string ToString()
    {
        return $"{nameof(DatabaseMode)} = {DatabaseMode}, {nameof(SearchEngine)} = {SearchEngine}";
    }
}
