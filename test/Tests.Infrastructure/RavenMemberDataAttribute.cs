using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Util;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure;

public class RavenMemberDataAttribute : MemberDataAttributeBase
{
    public RavenSearchEngineMode SearchEngineMode { get; set; } = RavenSearchEngineMode.Lucene;

    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.Single;

    public object[] Data { get; set; } = null;

    public RavenMemberDataAttribute(string memberName, params object[] parameters) : base(memberName, parameters)
    {
    }

    public override bool SupportsDiscoveryEnumeration() => false;

    public override async ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var baseData = await base.GetData(testMethod, disposalTracker);
        var result = new List<ITheoryDataRow>();

        foreach (var row in baseData)
        {
            var item = row.GetData();
            foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
            {
                foreach (var (searchMode, o) in RavenDataAttribute.FillOptions(options, SearchEngineMode))
                {
                    using (SkipIfNeeded(databaseMode))
                    {
                        var length = item.Length + 1;
                        if (Data is { Length: > 0 })
                            length += Data.Length;

                        var array = new object[length];

                        array[0] = o;

                        for (int i = 0; i < item.Length; i++)
                        {
                            array[i + 1] = item[i];
                        }

                        for (var i = item.Length + 1; i < array.Length; i++)
                            array[i] = Data[i - 1];

                        result.Add(new TheoryDataRow(array));
                    }
                }
            }
        }

        return result;
    }

    private IDisposable SkipIfNeeded(RavenDatabaseMode databaseMode)
    {
        if (RavenDataAttributeBase.CanContinue(databaseMode, Skip))
        {
            return null;
        }

        Skip = RavenDataAttributeBase.ShardingSkipMessage;
        return new DisposableAction(() => Skip = null);
    }
}
