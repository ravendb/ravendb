namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class BuildVersion
    {
        public static BuildVersionType Type(long buildVersion)
        {
            if (buildVersion >= 50 && buildVersion < 60)
                return BuildVersionType.V5; // debug / dev version
            if (buildVersion >= 40 && buildVersion < 50)
                return BuildVersionType.V4; // debug / dev version
            if (buildVersion < 40000)
                return BuildVersionType.V3;
            if (buildVersion >= 40000 && buildVersion <= 49999)
                return BuildVersionType.V4;
            if (buildVersion >= 50000 && buildVersion <= 59999)
                return BuildVersionType.V5;
            return BuildVersionType.Unknown;
        }
    }

    public enum BuildVersionType
    {
        Unknown,
        V3,
        V4,
        V5
    }
}
