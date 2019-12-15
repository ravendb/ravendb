using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable
    {
        ITimeSeriesLoadQueryable<TTag> LoadTag<TTag>();

        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesValue, bool>> predicate);

        ITimeSeriesGroupByQueryable GroupBy(string s);

        ITimeSeriesGroupByQueryable GroupBy(Action<ITimeSeriesGroupByBuilder> timePeriod);

        TimeSeriesRaw ToList();

    }

    public interface ITimeSeriesLoadQueryable<T> : ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesValue, T, bool>> predicate);
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

    public interface ITimeSeriesGroupByBuilder
    {
        void Milliseconds(int duration);

        void Seconds(int duration);

        void Minutes(int duration);

        void Hours(int duration);

        void Days(int duration);

        void Months(int duration);

        void Quarters(int duration);

        void Years(int duration);

    }

}
