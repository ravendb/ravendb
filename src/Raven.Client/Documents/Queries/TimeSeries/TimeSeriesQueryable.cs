using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable<T>
    {
        ITimeSeriesQueryable<T> Where(Expression<Func<TimeSeriesValue, bool>> predicate);

        ITimeSeriesQueryable<T> GroupBy(string s);

        ITimeSeriesQueryable<T> Select(Expression<Func<ITimeSeriesQueryable<T>, object>> selector);

        T ToList();
    }

}
