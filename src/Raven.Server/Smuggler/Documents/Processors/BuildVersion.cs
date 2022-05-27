using Raven.Server.ServerWide;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class BuildVersion
    {
        public static BuildVersionType Type(long buildVersion)
        {
            return buildVersion switch
            {
                >= 60 and < 1000 => BuildVersionType.GreaterThanCurrent,
                >= 50 and < 60 => BuildVersionType.V5,
                >= 40 and < 50 => BuildVersionType.V4,
                >= 60000 => BuildVersionType.GreaterThanCurrent,
                >= 50000 and <= 59999 => BuildVersionType.V5,
                >= 40000 and <= 49999 => BuildVersionType.V4,
                < 40000 => BuildVersionType.V3
            };
        }
    }

    public enum BuildVersionType
    {
        Unknown,
        V3,
        V4,
        V5,
        GreaterThanCurrent
    }
}
