namespace Raven.Server.Documents.Handlers.AI.Agents;

// Server-side counterpart of the client's AiOutputOptions.
// On the wire SampleObject always arrives as a JSON string, so here it is typed as string (the client side uses object).
public sealed class AiServerOutputOptions
{
    public string SampleObject { get; set; }
    public string OutputSchema { get; set; }
    public bool NoSchema { get; set; }
}
