using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

public class GenerationInfo : IDynamicJson
{
    public string GenerationName { get; set; }
    public long FragmentationAfterBytes { get; set; }
    public string FragmentationAfterHumane { get; set; }
    public long FragmentationBeforeBytes { get; set; }
    public string FragmentationBeforeHumane { get; set; }
    public long SizeAfterBytes { get; set; }
    public string SizeAfterHumane { get; set; }
    public long SizeBeforeBytes { get; set; }
    public string SizeBeforeHumane { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(GenerationName)] = GenerationName,
            [nameof(FragmentationAfterBytes)] = FragmentationAfterBytes,
            [nameof(FragmentationAfterHumane)] = FragmentationAfterHumane,
            [nameof(FragmentationBeforeBytes)] = FragmentationBeforeBytes,
            [nameof(FragmentationBeforeHumane)] = FragmentationBeforeHumane,
            [nameof(SizeAfterBytes)] = SizeAfterBytes,
            [nameof(SizeAfterHumane)] = SizeAfterHumane,
            [nameof(SizeBeforeBytes)] = SizeBeforeBytes,
            [nameof(SizeBeforeHumane)] = SizeBeforeHumane
        };
    }
}
