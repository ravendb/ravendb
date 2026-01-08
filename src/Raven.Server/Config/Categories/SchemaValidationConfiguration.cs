using System;
using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.SchemaValidation)]
    public sealed class SchemaValidationConfiguration : ConfigurationCategory
    {
        public SchemaValidationConfiguration()
        {
        }

        [Description("The maximum depth allowed for document schema validation")]
        [DefaultValue(64)]
        [MinValue(1)]
        [ConfigurationEntry("SchemaValidation.MaxDepth", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxDepth { get; set; }

        [Description("The maximum time allowed for regular expression matching during schema validation (in milliseconds)")]
        [DefaultValue(1000)]
        [MinValue(10)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("SchemaValidation.RegexTimeoutInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting RegexTimeout { get; set; }
    }
}
