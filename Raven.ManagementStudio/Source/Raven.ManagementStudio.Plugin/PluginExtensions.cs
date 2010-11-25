namespace Raven.ManagementStudio.Plugin
{
    using System;

    public static class PluginExtensions
    {
        public static string GetName(this SectionType section)
        {
            return Enum.GetName(typeof (SectionType), section);
        }
    }
}