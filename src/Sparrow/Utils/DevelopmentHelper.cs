namespace Sparrow.Utils;

internal static class DevelopmentHelper
{
    internal static void ToDo(Feature feature, TeamMember member, Severity severity, string message)
    {
        // nothing to do here
    }

    internal static void ShardingToDo(TeamMember member, Severity severity, string message) => ToDo(Feature.Sharding, member, severity, message);

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
        Grisha,
        Efrat,
        Marcin,
        Egor,
        Stav
    }

    internal enum Severity
    {
        Minor,
        Normal,
        Major,
        Critical
    }
}
