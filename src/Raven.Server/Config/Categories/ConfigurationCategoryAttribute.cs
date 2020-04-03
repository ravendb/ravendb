using System;

namespace Raven.Server.Config.Categories
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ConfigurationCategoryAttribute : Attribute
    {
        public ConfigurationCategoryType Type { get; set; }

        public ConfigurationCategoryAttribute(ConfigurationCategoryType type)
        {
            Type = type;
        }
    }
}
