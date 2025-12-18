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

        [Description("The maximum time allowed for a single document schema validation (in milliseconds)")]
        [DefaultValue(500)]
        [MinValue(10)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("SchemaValidation.DocumentValidationTimeoutInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting ValidationTimeout { get; set; }

        [Description("The maximum depth allowed for document schema validation")]
        [DefaultValue(16)]
        [MinValue(1)]
        [ConfigurationEntry("SchemaValidation.MaxDepth", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxDepth { get; set; }

        [Description("The maximum time allowed for regular expression matching during schema validation (in milliseconds)")]
        [DefaultValue(200)]
        [MinValue(10)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("SchemaValidation.RegexTimeoutInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting RegexTimeout { get; set; }
    }
}
