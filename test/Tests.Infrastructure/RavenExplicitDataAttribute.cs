using System.Collections.Generic;
using System.Reflection;
using FastTests;
using Xunit.Sdk;

namespace Tests.Infrastructure;

public class RavenExplicitDataAttribute : DataAttribute
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

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
        {
            foreach (var (searchMode, o) in RavenDataAttribute.FillSearchOptions(options, SearchEngineMode))
            {
                var length = 1;
                if (Data is { Length: > 0 })
                    length += Data.Length;

                var array = new object[length];

                array[0] = new RavenTestParameters
                {
                    SearchEngine = searchMode,
                    DatabaseMode = databaseMode,
                    Options = o
                };

                for (var i = 1; i < array.Length; i++)
                    array[i] = Data[i - 1];

                yield return array;
            }
        }
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
