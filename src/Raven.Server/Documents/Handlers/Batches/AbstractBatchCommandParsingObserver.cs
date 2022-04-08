using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Batches;

public abstract class AbstractBatchCommandParsingObserver
{
    public abstract void OnCommandStart(UnmanagedJsonParser parser);

    public abstract void OnCommandEnd(UnmanagedJsonParser parser);

    public abstract void OnParserBufferRefill(UnmanagedJsonParser parser);

    public abstract void OnId(UnmanagedJsonParser parser, int idLength);

    public abstract void OnIdsStart(UnmanagedJsonParser parser);

    public abstract void OnIdsEnd(UnmanagedJsonParser parser);

    public abstract void OnNullChangeVector(UnmanagedJsonParser parser);
}
