using System;
using System.Collections.Generic;
using Raven.Database.Data;

namespace Raven.Client
{
	public interface IDocumentQuery<T> : IEnumerable<T>
	{
		IDocumentQuery<T> Take(int count);
		IDocumentQuery<T> Skip(int count);
		IDocumentQuery<T> Where(string whereClause);
		IDocumentQuery<T> OrderBy(params string[] fields);

        IDocumentQuery<T> WaitForNonStaleResultsAsOfNow();
        IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

        IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff);
        IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout);
		

		IDocumentQuery<T> WaitForNonStaleResults();
        IDocumentQuery<T> WaitForNonStaleResults(TimeSpan waitTimeout);
		IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

		QueryResult QueryResult { get; }
	}
}