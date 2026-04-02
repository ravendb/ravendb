using System;
using Raven.Client.Documents.Operations.Revisions;

namespace Raven.Server.Documents.Revisions
{
    public sealed class RevertRevisionsRequest : RevisionsOperationParameters
    {
        public DateTime Time { get; set; }
        public long WindowInSec { get; set; }
    }
}
