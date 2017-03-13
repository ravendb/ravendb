namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class BuildVersion
    {
        public static bool IsV4(long buildVersion)
        {
            return buildVersion >= 40000 && buildVersion <= 44999 || buildVersion == 40;
        }

        public static bool IsPreV4(long buildVersion)
        {
            return buildVersion == 0;
        }
    }
}