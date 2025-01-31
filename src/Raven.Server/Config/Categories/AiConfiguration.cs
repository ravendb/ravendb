using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.Ai)]
public sealed class AiConfiguration : ConfigurationCategory
{
    [Description("Max number of extracted documents in AI integration batch")]
    [DefaultValue(8192)]
    [ConfigurationEntry("Ai.MaxNumberOfExtractedDocuments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
    public int? MaxNumberOfExtractedDocuments { get; set; }
}
