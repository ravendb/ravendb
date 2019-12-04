using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesValue, bool>> predicate);

        ITimeSeriesQueryable GroupBy(string s);

        TimeSeriesAggregation Select(Expression<Func<ITimeSeriesQueryable, object>> selector);
    }

}
