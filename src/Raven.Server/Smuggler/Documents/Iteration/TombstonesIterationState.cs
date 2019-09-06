using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public class TombstonesIterationState : IterationState<Tombstone>
    {
        public TombstonesIterationState(DocumentsOperationContext context) : base(context)
        {
        }

        public long StartEtag;

        public Dictionary<string, long> StartEtagByCollection = new Dictionary<string, long>();

        public string CurrentCollection;

        public override void OnMoveNext(Tombstone current)
        {
            if (StartEtagByCollection.Count != 0)
            {
                StartEtagByCollection[CurrentCollection] = current.Etag + 1;
            }
            else
            {
                StartEtag = current.Etag + 1;
            }

            ReadCount++;
        }
    }
}
