using System;
using System.Collections.Generic;
using Raven.Server.Documents;

namespace Raven.Server.Exceptions
{
    public class DocumentConflictException : Exception
    {
        public string DocId { get; private set; }
        public IReadOnlyList<DocumentConflict> Conflicts { get; private set; }

        public DocumentConflictException(string docId, IReadOnlyList<DocumentConflict> conflicts)
            :base($"Conflict detected on {docId}, conflict must be resolved before the document will be accessible")
        {
            DocId = docId;
            Conflicts = conflicts;            
        }
    }
}