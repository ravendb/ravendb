using System;
using System.Collections.Generic;
using System.Reflection;
using FastTests;
using Raven.Client.Util;
using Xunit;

namespace Tests.Infrastructure;

public class RavenMemberDataAttribute : MemberDataAttributeBase
{
    public RavenSearchEngineMode SearchEngineMode { get; set; } = RavenSearchEngineMode.Lucene;

    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.Single;

    public object[] Data { get; set; } = null;
    
    public RavenMemberDataAttribute(string memberName, params object[] parameters) : base(memberName, parameters)
    {
    }

    protected override object[] ConvertDataItem(MethodInfo testMethod, object item)
    {
        if (item == null)
            return null;

        var array = item as object[];
        if (array == null)
            throw new ArgumentException($"Property {MemberName} on {MemberType ?? testMethod.DeclaringType} yielded an item that is not an object[]");

        return array;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        foreach (var item in base.GetData(testMethod))
            foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
            {
                foreach (var (searchMode, o) in RavenDataAttribute.FillOptions(options, SearchEngineMode))
                {
                    using (SkipIfNeeded(o))
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

                        yield return array;
                    }
                }
            }
    }

    protected IDisposable SkipIfNeeded(RavenTestBase.Options options)
    {
        if (string.IsNullOrEmpty(Skip) == false)
        {
            // test skipped explicitly in attribute
            return null;
        }

        if (string.IsNullOrEmpty(options.Skip))
        {
            // no skip in options
            return null;
        }

        var s = Skip;
        Skip = options.Skip;
        return new DisposableAction(() => Skip = s);
    }
}
