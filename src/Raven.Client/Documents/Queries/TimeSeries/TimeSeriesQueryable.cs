using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, bool>> predicate);

        ITimeSeriesQueryable Offset(TimeSpan offset);

        ITimeSeriesQueryable Scale(double value);

        ITimeSeriesQueryable FromLast(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesQueryable FromFirst(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesLoadQueryable<TEntity> LoadByTag<TEntity>();

        ITimeSeriesAggregationQueryable GroupBy(string s);

        ITimeSeriesAggregationQueryable GroupBy(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        TimeSeriesRawResult ToList();
    }

    public interface ITimeSeriesQueryable<T> where T : new()
    {
        ITimeSeriesQueryable<T> Where(Expression<Func<TimeSeriesEntry<T>, bool>> predicate);

        ITimeSeriesQueryable<T> Offset(TimeSpan offset);

        ITimeSeriesQueryable<T> Scale(double value);

        ITimeSeriesQueryable<T> FromLast(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesQueryable<T> FromFirst(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesLoadQueryable<T, TEntity> LoadByTag<TEntity>();

        ITimeSeriesAggregationQueryable<T> GroupBy(string s);

        ITimeSeriesAggregationQueryable<T> GroupBy(Action<ITimePeriodBuilder> timePeriod);

        ITimeSeriesAggregationQueryable<T> Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        TimeSeriesRawResult<T> ToList();
    }

    public interface ITimeSeriesAggregationQueryable
    {
        ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        ITimeSeriesAggregationQueryable Offset(TimeSpan offset);

        ITimeSeriesAggregationQueryable Scale(double value);

        TimeSeriesAggregationResult ToList();
    }

    public interface ITimeSeriesAggregationQueryable<T> where T : new()
    {
        ITimeSeriesAggregationQueryable<T> Select(Expression<Func<ITimeSeriesGrouping, object>> selector);

        ITimeSeriesAggregationQueryable<T> Offset(TimeSpan offset);

        ITimeSeriesAggregationQueryable<T> Scale(double value);

        TimeSeriesAggregationResult<T> ToList();
    }

    public interface ITimeSeriesLoadQueryable<TTag> : ITimeSeriesQueryable
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, TTag, bool>> predicate);
    }

    public interface ITimeSeriesLoadQueryable<T, TTag> : ITimeSeriesQueryable where T : new()
    {
        ITimeSeriesQueryable<T> Where(Expression<Func<TimeSeriesEntry<T>, TTag, bool>> predicate);
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

        double[] Percentile(double number);

        double[] Slope();

    }

    public interface ITimePeriodBuilder
    {
        ITimeSeriesAggregationOperations Milliseconds(int duration);

        ITimeSeriesAggregationOperations Seconds(int duration);

        ITimeSeriesAggregationOperations Minutes(int duration);

        ITimeSeriesAggregationOperations Hours(int duration);

        ITimeSeriesAggregationOperations Days(int duration);

        ITimeSeriesAggregationOperations Months(int duration);

        ITimeSeriesAggregationOperations Quarters(int duration);

        ITimeSeriesAggregationOperations Years(int duration);
    }

    public interface ITimeSeriesAggregationOperations
    {
        public void WithOptions(TimeSeriesAggregationOptions options);
        public ITimeSeriesAggregationOperations ByTag();
        public ITimeSeriesAggregationOperations ByTag<TEntity>(Expression<Func<TEntity, object>> selector);
    }
}
