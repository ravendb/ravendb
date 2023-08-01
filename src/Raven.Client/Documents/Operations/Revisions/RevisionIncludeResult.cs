using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    internal sealed class RevisionIncludeResult
    {
        public string Id { get; set; }
        public string ChangeVector { get; set; }
        public DateTime Before { get; set; }
        public BlittableJsonReaderObject Revision { get; set; }
    }
}
