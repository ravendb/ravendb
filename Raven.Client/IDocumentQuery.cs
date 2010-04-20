using System;
using System.Collections.Generic;
using Raven.Database.Data;

namespace Raven.Client
{
	public interface IDocumentQuery<out T> : IEnumerable<T>
	{
		IDocumentQuery<T> Take(int count);
		IDocumentQuery<T> Skip(int count);
		IDocumentQuery<T> Where(string whereClause);
		IDocumentQuery<T> OrderBy(params string[] fields);
		IDocumentQuery<T> WaitForNonStaleResults();
		IDocumentQuery<T> WaitForNonStaleResults(TimeSpan timeout);

	    IDocumentQuery<TProjection> Select<TProjection>(Func<T, TProjection> projectionExpression);

		QueryResult QueryResult { get; }
	}
}