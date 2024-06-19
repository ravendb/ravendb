using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Revisions
{
    internal class RevertDocumentsToRevisionsRequest
    {
        public Dictionary<string, string> IdToChangeVector { get; set; }
    }
}
