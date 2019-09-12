using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
using Sparrow;

namespace Raven.Server.Documents.Handlers.Streaming
{
    public class DocsStreamingIterationState : PulsedEnumerationState<Document>
    {
        public DocsStreamingIterationState(DocumentsOperationContext context, Size pulseLimit) : base(context, pulseLimit)
        {
        }

        public string StartsWith { get; set; }
        public string Excludes { get; set; }
        public string Matches { get; set; }
        public string StartAfter { get; set; }
        public int Start { get; set; }
        public int Take { get; set; }
        public long? LastIteratedEtag;
        public Reference<int> Skip;

        public override void OnMoveNext(Document current)
        {
            Take--;
            LastIteratedEtag = current.Etag;
        }
    }
}
