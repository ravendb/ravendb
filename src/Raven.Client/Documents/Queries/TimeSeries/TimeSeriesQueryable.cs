using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable<out T>
    {
        ITimeSeriesQueryable<T> LoadTag<T2>(out T2 alias);

        ITimeSeriesQueryable<T> Where(Expression<Func<TimeSeriesValue, bool>> predicate);

        ITimeSeriesQueryable<T> GroupBy(string s);

        ITimeSeriesQueryable<T> Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        T ToList();

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
