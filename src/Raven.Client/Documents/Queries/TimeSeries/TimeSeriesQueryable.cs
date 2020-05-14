using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable
    {
        ITimeSeriesLoadQueryable<TTag> LoadTag<TTag>();

        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, bool>> predicate);

        ITimeSeriesAggregationQueryable GroupBy(string s);

        ITimeSeriesAggregationQueryable GroupBy(Action<ITimeSeriesGroupByBuilder> timePeriod);

        ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        ITimeSeriesQueryable Offset(TimeSpan offset);

        TimeSeriesRawResult ToList();

        TimeSeriesRawResult<T> ToList<T>() where T : TimeSeriesEntry;
    }

    public interface ITimeSeriesLoadQueryable<T> : ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, T, bool>> predicate);
    }

    public interface ITimeSeriesAggregationQueryable
    {
        ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        ITimeSeriesAggregationQueryable Offset(TimeSpan offset);

        TimeSeriesAggregationResult ToList();

        TimeSeriesAggregationResult<T> ToList<T>() where T : TimeSeriesAggregatedEntry, new();
    }

    public interface ITimeSeriesGrouping
    {
        double[] Max();

        double[] Min();

        double[] Sum();

        double[] Average();

        double[] First();

        double[] Last();

        long[] Count();

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
