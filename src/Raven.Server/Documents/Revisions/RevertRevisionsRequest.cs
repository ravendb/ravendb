using System;
using System.Collections.Generic;
using Amqp.Types;

namespace Raven.Server.Documents.Revisions
{
    public class RevertRevisionsRequest
    {
        public DateTime Time { get; set; }
        public long WindowInSec { get; set; }
        public bool ApplyToSpecifiedCollectionsOnly { get; set; } = false;
        public string[] Collections { get; set; } = null;
    }
}
