using System;

namespace Raven.Server.Config.Attributes
{
    [Obsolete("This attribute should be used only to mark old configuration entry keys so in the future it would be easier to generate document that maps old keys to the new ones for documentation purposes.")]
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class LegacyConfigurationEntryAttribute : Attribute
    {
        public string Key { get; set; }

        public LegacyConfigurationEntryAttribute(string key)
        {
            Key = key;
        }
    }
}