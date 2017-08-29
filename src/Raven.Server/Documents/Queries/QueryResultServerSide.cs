using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries
{
    public abstract class QueryResultServerSide<T> : QueryResult<List<T>, List<T>>
    {
        protected QueryResultServerSide()
        {
            Results = new List<T>();
            Includes = new List<T>();
        }

        public abstract void AddResult(T result);

        public abstract void HandleException(Exception e);

        public abstract bool SupportsExceptionHandling { get; }

        public abstract bool SupportsInclude { get; }

        public bool NotModified { get; protected set; }
    }

    public abstract class QueryResultServerSide : QueryResultServerSide<Document>
    {
    }
}
