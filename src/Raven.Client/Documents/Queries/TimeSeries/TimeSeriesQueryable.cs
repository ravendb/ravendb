using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable
    {
        ITimeSeriesLoadQueryable<T2> LoadTag<T2>();

        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesValue, bool>> predicate);

        ITimeSeriesGroupByQueryable GroupBy(string s);

        TimeSeriesRaw ToList();

    }

    public interface ITimeSeriesLoadQueryable<T>
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesValue, T, bool>> predicate);

        ITimeSeriesGroupByQueryable GroupBy(string s);

        TimeSeriesRaw ToList();

    }

    public interface ITimeSeriesGroupByQueryable
    {
        ITimeSeriesGroupByQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        TimeSeriesAggregation ToList();

    }

    public interface ITimeSeriesGrouping
    {
        double?[] Max();

        double?[] Min();

        double?[] Sum();

        double?[] Average();

        double?[] First();

        double?[] Last();

        double?[] Count();

    }

}
