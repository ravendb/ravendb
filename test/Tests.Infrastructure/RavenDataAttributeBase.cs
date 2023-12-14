using System;
using FastTests;
using Raven.Client.Util;
using Xunit.Sdk;

namespace Tests.Infrastructure;

public abstract class RavenDataAttributeBase : DataAttribute
{
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
