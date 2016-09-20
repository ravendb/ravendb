using System;
using Raven.Client.Data.Queries;

namespace Raven.Server.Documents.Queries
{
    public abstract class QueryResultServerSide : QueryResult<Document>
    {
        public abstract void AddResult(Document result);

        public abstract void HandleException(Exception e);

        public abstract bool SupportsExceptionHandling { get; }

        public abstract bool SupportsInclude { get; }

        public bool NotModified { get; protected set; }
    }
}