using System;

namespace Raven.Server.Smuggler.Documents.Processors
{
    /*public static class BuildVersion
    {
        public static bool IsV4(long buildVersion)
        {
            return buildVersion >= 40000 && buildVersion <= 44999 || buildVersion == 40;
        }

        public static bool IsPreV4(long buildVersion)
        {
            return buildVersion == 0;
        }
    }*/

    public static class BuildVersion
    {
        public static BuildVersionType Type(long buildVersion)
        {
            BuildVersionType type;

            if (buildVersion < 40000)
                type = BuildVersionType.V3;
            else if (buildVersion >= 40000 && buildVersion <= 49999 || buildVersion == 40 || buildVersion == 45)
                type = BuildVersionType.V4;
            else
                type = BuildVersionType.Unknown;

            return type;
        }
    }

    public enum BuildVersionType
    {
        Unknown,
        V3,
        V4
    }
}