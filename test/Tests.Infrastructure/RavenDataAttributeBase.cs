using System;
using System.Runtime.InteropServices;
using Raven.Client.Util;
using Xunit.Sdk;

namespace Tests.Infrastructure;

public abstract class RavenDataAttributeBase : DataAttribute
{
    internal static readonly bool Is32Bit = RuntimeInformation.ProcessArchitecture == Architecture.X86;
    internal const string ShardingSkipMessage = "RavenDB-19879: Skip Sharded database tests on x86 architecture.";

    protected IDisposable SkipIfNeeded(RavenDatabaseMode databaseMode)
    {
        if (CanContinue(databaseMode, Skip))
            return null;

        Skip = ShardingSkipMessage;
        return new DisposableAction(() => Skip = null);
    }

    public static bool CanContinue(RavenDatabaseMode databaseMode, string skip)
    {
        if (Is32Bit == false)
            return true;

        if (databaseMode.HasFlag(RavenDatabaseMode.Sharded) == false)
            return true;

        if (string.IsNullOrEmpty(skip) == false)
        {
            // test skipped explicitly in attribute
            return true;
        }

        return false;
    }
}
