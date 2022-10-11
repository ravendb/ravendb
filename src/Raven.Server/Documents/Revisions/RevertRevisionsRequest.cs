using System;
using System.Collections.Generic;
using Amqp.Types;

namespace Raven.Server.Documents.Revisions
{
    public class RevertRevisionsRequest
    {
        public DateTime Time { get; set; }
        public long WindowInSec { get; set; }
        public bool PerCollections { get; set; } = false;
        public List<string> Collections { get; set; } = null;
    }
}
