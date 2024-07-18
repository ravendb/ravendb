using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public abstract partial class HandleReferencesBase
    {
        public sealed record ReferenceState(string ReferencedItemId, long ReferencedItemEtag, string NextItemId, long LastIndexedParentEtag)
        {
            public string GetLastProcessedItemId(Reference referencedDocument)
            {
                if (referencedDocument.Key == ReferencedItemId && referencedDocument.Etag == ReferencedItemEtag)
                    return NextItemId;

                return null;
            }

            public long GetLastIndexedParentEtag()
            {
                return LastIndexedParentEtag;
            }
        }

        public record State(
            ImmutableDictionary<string, ReferenceState> Map,
            ImmutableDictionary<string, ReferenceState>.Builder Owner // only used by write transactions
        )
        {
            /// <summary>
            /// Cannot use a shared instance here, since we have a mutable builder, we create a new value each time
            /// </summary>
            public static State CreateEmpty() => new(ImmutableDictionary<string, ReferenceState>.Empty, ImmutableDictionary<string, ReferenceState>.Empty.ToBuilder());
        }
    }
}
