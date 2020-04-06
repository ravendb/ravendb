using Raven.Server.ServerWide;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class BuildVersion
    {
        public static BuildVersionType Type(long buildVersion)
        {
            if (buildVersion >= 50 && buildVersion < 1000)
                return BuildVersionType.GreaterThanCurrent; // debug / dev version
            if (buildVersion >= 40 && buildVersion < 50)
                return BuildVersionType.V4; // debug / dev version
            if (buildVersion < 40000)
                return BuildVersionType.V3;
            if (buildVersion >= 40000 && buildVersion <= 49999)
                return BuildVersionType.V4;
            if (buildVersion >= 50000)
                return BuildVersionType.GreaterThanCurrent;
            return BuildVersionType.Unknown;
        }
    }

    public enum BuildVersionType
    {
        Unknown,
        V3,
        V4,
        GreaterThanCurrent
    }
}
