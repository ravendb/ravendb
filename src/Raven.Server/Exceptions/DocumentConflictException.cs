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
            foreach (var conflict in conflicts)
            {
                // removing fields that are refeencing memory that is likely to be transaction local
                // and we are going to throw an exception, so we'll be throwing it outside the scope
                // of the transaction, and we mustn't leak this infromation
                conflict.Key = null;
                conflict.LoweredKey = null;
                conflict.Doc = null;
            }
        }
    }
}