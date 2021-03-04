using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public partial class JsonOperationContext
    {
        private SyncJsonOperationContext _sync;

        internal SyncJsonOperationContext Sync
        {
            get => _sync ??= new SyncJsonOperationContext(this);
        }

        internal class SyncJsonOperationContext
        {
            internal readonly JsonOperationContext Context;

            internal SyncJsonOperationContext(JsonOperationContext context)
            {
                Context = context;
            }

            internal void EnsureNotDisposed()
            {
                Context.EnsureNotDisposed();
            }

            internal JsonParserState JsonParserState => Context._jsonParserState;

            internal ObjectJsonParser ObjectJsonParser => Context._objectJsonParser;

            internal BlittableJsonDocumentBuilder DocumentBuilder => Context._documentBuilder;
        }
    }
}
