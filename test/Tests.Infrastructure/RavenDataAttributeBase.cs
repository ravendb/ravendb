using System.Runtime.InteropServices;
using Xunit.v3;

namespace Tests.Infrastructure;

public abstract class RavenDataAttributeBase : DataAttribute
{
    public override bool SupportsDiscoveryEnumeration() => false;

    internal static readonly bool Is32Bit = RuntimeInformation.ProcessArchitecture == Architecture.X86;
    internal const string ShardingSkipMessage = "RavenDB-19879: Skip Sharded database tests on x86 architecture.";

    protected string GetSkipReason(RavenDatabaseMode databaseMode) => GetSkipReason(databaseMode, Skip);

    internal static string GetSkipReason(RavenDatabaseMode databaseMode, string attributeSkip)
    {
        return CanContinue(databaseMode, attributeSkip) ? null : ShardingSkipMessage;
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
