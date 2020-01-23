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

        public string StartsWith;
        public string Excludes;
        public string Matches;
        public string StartAfter;
        public int Start;
        public int Take;
        public long? LastIteratedEtag;
        public Reference<long> Skip;

        public override void OnMoveNext(Document current)
        {
            Take--;
            LastIteratedEtag = current.Etag;
            ReadCount++;

            if (Skip != null)
                Skip.Value++;
        }
    }
}
