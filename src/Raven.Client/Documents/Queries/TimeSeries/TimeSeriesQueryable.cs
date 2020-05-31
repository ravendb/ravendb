using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, bool>> predicate);

        ITimeSeriesQueryable Offset(TimeSpan offset);

        ITimeSeriesQueryable FromLast(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesQueryable FromFirst(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesLoadQueryable<TTag> LoadTag<TTag>();

        ITimeSeriesAggregationQueryable GroupBy(string s);

        ITimeSeriesAggregationQueryable GroupBy(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        TimeSeriesRawResult ToList();
    }

    public interface ITimeSeriesQueryable<T> where T : new()
    {
        ITimeSeriesQueryable<T> Where(Expression<Func<TimeSeriesEntry, bool>> predicate);

        ITimeSeriesQueryable<T> Offset(TimeSpan offset);

        ITimeSeriesQueryable<T> FromLast(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesQueryable<T> FromFirst(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesLoadQueryable<T, TTag> LoadTag<TTag>();

        ITimeSeriesAggregationQueryable<T> GroupBy(string s);

        ITimeSeriesAggregationQueryable<T> GroupBy(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesAggregationQueryable<T> Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        TimeSeriesRawResult<T> ToList();
    }

    public interface ITimeSeriesAggregationQueryable
    {
        ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        ITimeSeriesAggregationQueryable Offset(TimeSpan offset);

        TimeSeriesAggregationResult ToList();
    }

    public interface ITimeSeriesAggregationQueryable<T> where T : new()
    {
        ITimeSeriesAggregationQueryable<T> Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        ITimeSeriesAggregationQueryable<T> Offset(TimeSpan offset);

        TimeSeriesAggregationResult<T> ToList();
    }

    public interface ITimeSeriesLoadQueryable<TTag> : ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, TTag, bool>> predicate);
    }

    public interface ITimeSeriesLoadQueryable<T, TTag> : ITimeSeriesQueryable where T : new()
    {
        ITimeSeriesQueryable<T> Where(Expression<Func<TimeSeriesEntry, TTag, bool>> predicate);
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

    public interface ITimePeriodBuilder
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
