using System.Net.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// The state one streamed response shares between ChatCompletionClient and the provider. Each provider derives its own
// state from this class (CreateStreamState) and keeps its wire-specific accounting there.
internal abstract class ChatStreamState
{
    public HttpResponseMessage Response;

    public bool StructuredOutput;

    public SseStreamingJsonParser Parser;           // owned by the client; read by the provider for IsInvalid

    public BlittableJsonReaderObject FinalResult;   // set by the client when the answer JSON parser completes

    public string StopReason;

    public bool SawStop;                            // the provider signaled the end of the response ([DONE] / message_stop)

    public string PendingChunk;                     // text the provider produced at the end that the client still has to stream
}
