using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQuery<T>
    {
        ITimeSeriesQuery<T> Where(Expression<Func<TimeSeriesValue, bool>> predicate);

        ITimeSeriesQuery<T> GroupBy(string s);

        T Select(Expression<Func<ITimeSeriesQuery<T>, object>> selector);
    }

}
