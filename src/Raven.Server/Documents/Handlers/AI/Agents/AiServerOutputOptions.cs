using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

// Server-side counterpart of the client's AiOutputOptions.
// On the wire SampleObject always arrives as a JSON string, so here it is typed as string (the client side uses object).
public sealed class AiServerOutputOptions : AiOutputOptions
{
    public new string SampleObject { get; set; }

    public static AiServerOutputOptions From(BlittableJsonReaderObject bjro)
    {
        if (bjro.TryGet(nameof(ConversionRequestBody.OutputOptions), out BlittableJsonReaderObject outputOptions) && outputOptions != null)
        {
            var opts = new AiServerOutputOptions();
            if (outputOptions.TryGet(nameof(AiServerOutputOptions.NoSchema), out bool noSchema) && noSchema)
            {
                opts.NoSchema = true;
            }
            else if (outputOptions.TryGet(nameof(AiServerOutputOptions.OutputSchema), out string outputSchema) && string.IsNullOrWhiteSpace(outputSchema) == false)
            {
                opts.OutputSchema = outputSchema;
            }
            else if (outputOptions.TryGet(nameof(AiServerOutputOptions.SampleObject), out string sampleObject) && string.IsNullOrWhiteSpace(sampleObject) == false)
            {
                opts.SampleObject = sampleObject;
            }
            return opts;
        }

        return null;
    }
}
