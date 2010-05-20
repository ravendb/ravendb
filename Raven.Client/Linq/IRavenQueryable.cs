using System;
using System.Linq;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
    public interface IRavenQueryable<T> : IOrderedQueryable<T>
    {
        IRavenQueryable<T> Customize(Action<IDocumentQuery<T>> action);
	
		QueryResult QueryResult { get; }
	}
}