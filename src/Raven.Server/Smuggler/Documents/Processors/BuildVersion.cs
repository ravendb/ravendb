
namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class BuildVersion
    {
        public static BuildVersionType Type(long buildVersion)
        {
            return buildVersion switch
            {
                >= 80 and < 1000 => BuildVersionType.GreaterThanCurrent,
                >= 70 and < 80 => BuildVersionType.V7,
                >= 60 and < 70 => BuildVersionType.V6,
                >= 50 and < 60 => BuildVersionType.V5,
                >= 40 and < 50 => BuildVersionType.V4,
                >= 70000 => BuildVersionType.GreaterThanCurrent,
                >= 60000 and <= 69999 => BuildVersionType.V6,
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
        V6,
        V7,
        GreaterThanCurrent
    }
}
