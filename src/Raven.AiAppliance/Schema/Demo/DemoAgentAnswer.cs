namespace Raven.AiAppliance.Schema.Demo;

/// Public fields with example values are how RavenDB derives the JSON schema
/// sent to the LLM (per the AiAgentBasics test pattern). Keep them as fields
/// with initializers — auto-properties bypass the schema generator.
public sealed class DemoAgentAnswer
{
    public static readonly DemoAgentAnswer Sample = new();

    public string Reply = "Friendly reply for the user.";
    public List<string> Related = ["Documents/<id> the user might want to read next"];
}
