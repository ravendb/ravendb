namespace Sparrow.Utils;

internal static class DevelopmentHelper
{
    internal static void ToDo(Feature feature, TeamMember member, Severity severity, string message)
    {
        // nothing to do here
    }

    internal enum Feature
    {
        Sharding
    }

    internal enum TeamMember
    {
        Karmel,
        Aviv,
        Pawel,
        Arek,
        Grisha
    }

    internal enum Severity
    {
        Minor,
        Normal,
        Major,
        Critical
    }
}
