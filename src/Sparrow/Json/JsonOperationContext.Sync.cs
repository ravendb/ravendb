using Sparrow.Json.Parsing;
using static Sparrow.DisposableExceptions;

namespace Sparrow.Json
{
    public partial class JsonOperationContext
    {
        private SyncJsonOperationContext _sync;

        internal SyncJsonOperationContext Sync
        {
            get => _sync ??= new SyncJsonOperationContext(this);
        }

        internal sealed class SyncJsonOperationContext
        {
            internal readonly JsonOperationContext Context;

            internal SyncJsonOperationContext(JsonOperationContext context)
            {
                Context = context;
            }

            internal void EnsureNotDisposed()
            {
                ThrowIfDisposed(Context);
            }

            internal JsonParserState JsonParserState => Context._jsonParserState;

            internal ObjectJsonParser ObjectJsonParser => Context._objectJsonParser;

            internal BlittableJsonDocumentBuilder DocumentBuilder => Context._documentBuilder;
        }
    }
}
