using System;
using System.Linq;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
    public interface IRavenQueryProvider : IQueryProvider
    {
        void Customize(Delegate action);
        IDocumentSession Session { get; }
        string IndexName { get; }
    	QueryResult QueryResult { get; }
    }
}