using System;
using System.IO;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents
{
    public class Counter
    {
        public long StorageId;
        public LazyStringValue Key;
        public LazyStringValue Name;
        public Guid DbId; // ?
        public long Etag;
        public long Value;
        public long SourceEtag; // ?
        public short TransactionMarker;
    }
}
