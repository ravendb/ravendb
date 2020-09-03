using System;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public interface ITimeSeriesQueryBuilder : ITimeSeriesQueryable
    {
        T Raw<T>(string queryText) where T : TimeSeriesQueryResult;

        ITimeSeriesQueryBuilder From(string name);

        ITimeSeriesQueryBuilder Between(DateTime start, DateTime end);

        new ITimeSeriesQueryBuilder FromLast(Action<ITimePeriodBuilder> timePeriod);

        new ITimeSeriesQueryBuilder FromFirst(Action<ITimePeriodBuilder> timePeriod);

        new ITimeSeriesLoadTagBuilder<TTag> LoadByTag<TTag>();
    }

    public interface ITimeSeriesLoadTagBuilder<TTag>
    {
        ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, TTag, bool>> predicate);
    }

    internal class TimeSeriesLoadTagBuilder<TEntity, TTag> : ITimeSeriesLoadTagBuilder<TTag>
    {
        private readonly TimeSeriesQueryBuilder<TEntity> _parent;

        public TimeSeriesLoadTagBuilder(TimeSeriesQueryBuilder<TEntity> builder)
        {
            _parent = builder;
        }

        public ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, TTag, bool>> predicate)
        {
            return _parent.Where(predicate);
        }
    }

    internal class TimeSeriesQueryBuilder<TEntity> : ITimeSeriesQueryBuilder, ITimeSeriesAggregationQueryable
    {
        private string _query;
        private string _name;
        private DateTime? _start;
        private DateTime? _end;
        private MethodCallExpression _callExpression;
        private readonly IAbstractDocumentQuery<TEntity> _documentQuery;
        private readonly LinqPathProvider _linqPathProvider;

        public string QueryText => _query;

        public TimeSeriesQueryBuilder(IAbstractDocumentQuery<TEntity> abstractDocumentQuery, LinqPathProvider linqPathProvider)
        {
            _documentQuery = abstractDocumentQuery;
            _linqPathProvider = linqPathProvider;
        }

        public T Raw<T>(string queryText) where T : TimeSeriesQueryResult
        {
            _query = queryText;
            return default;
        }

        /*private void Act<T>([CallerMemberName] string name = null, Expression arg = null)
        {
            switch (name)
            {
                case nameof(ITimeSeriesQueryBuilder.Where):
                    break;
                case nameof(ITimeSeriesQueryBuilder.LoadByTag):
                    break;
                case nameof(ITimeSeriesQueryBuilder.GroupBy):
                    break;
                case nameof(ITimeSeriesQueryBuilder.Select):
                    break;
                case nameof(ITimeSeriesQueryBuilder.Offset):
                    break;
                case nameof(ITimeSeriesQueryBuilder.Scale):
                    break;
                case nameof(ITimeSeriesQueryBuilder.FromFirst):
                    break;
                case nameof(ITimeSeriesQueryBuilder.FromLast):
                    break;

                default:
                    throw new ArgumentException(name);
            }
        }*/

        public ITimeSeriesQueryBuilder From(string name)
        {
            _name = name;

            var methodInfo = typeof(RavenQuery)
                .GetMethods()
                .SingleOrDefault(method => 
                    method.Name == nameof(RavenQuery.TimeSeries) && 
                    method.IsGenericMethod == false && 
                    method.GetParameters().Length == 1);

            Debug.Assert(methodInfo != null);

            _callExpression = Expression.Call(
                methodInfo,
                Expression.Constant(_name)
            );

            return this;
        }

        public ITimeSeriesQueryBuilder Between(DateTime start, DateTime end)
        {
            _start = start;
            _end = end;

            var methodInfo = typeof(RavenQuery)
                .GetMethods()
                .SingleOrDefault(method =>
                    method.Name == nameof(RavenQuery.TimeSeries) &&
                    method.IsGenericMethod == false &&
                    method.GetParameters().Length == 4);


            Debug.Assert(methodInfo != null);

            _callExpression = Expression.Call(
                methodInfo,
                Expression.Constant(null),
                Expression.Constant(_name),
                Expression.Constant(_start),
                Expression.Constant(_end)
            );

            return this;
        }

        public ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, bool>> predicate)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.Where));

            ModifyCallExpression(methodInfo, predicate);

            return this;
        }

        internal ITimeSeriesQueryable Where<TTag>(Expression<Func<TimeSeriesEntry, TTag, bool>> predicate)
        {
            var methodInfo = typeof(ITimeSeriesLoadQueryable<TTag>)
                .GetMethod(nameof(ITimeSeriesLoadQueryable<TTag>.Where));

            ModifyCallExpression(methodInfo, predicate);

            return this;
        }

        ITimeSeriesLoadTagBuilder<TTag> ITimeSeriesQueryBuilder.LoadByTag<TTag>()
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.LoadByTag))?
                .MakeGenericMethod(typeof(TTag));

            ModifyCallExpression(methodInfo);

            return new TimeSeriesLoadTagBuilder<TEntity, TTag>(this);
        }

        public ITimeSeriesAggregationQueryable GroupBy(string s)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.GroupBy), new[] { typeof(string) });
            ModifyCallExpression(methodInfo, Expression.Constant(s));
            return this;
        }

        public ITimeSeriesAggregationQueryable GroupBy(Action<ITimePeriodBuilder> timePeriod)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.GroupBy), new[] { typeof(Action<ITimePeriodBuilder>) });

            ModifyCallExpression(methodInfo, Expression.Constant(timePeriod));

            return this;
        }

        ITimeSeriesAggregationQueryable ITimeSeriesQueryable.Select(Expression<Func<ITimeSeriesGrouping, object>> selector)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesAggregationQueryable.Select));

            ModifyCallExpression(methodInfo, selector);

            return this;
        }

        ITimeSeriesAggregationQueryable ITimeSeriesAggregationQueryable.Select(Expression<Func<ITimeSeriesGrouping, object>> selector)
        {
            var methodInfo = typeof(ITimeSeriesAggregationQueryable)
                .GetMethod(nameof(ITimeSeriesAggregationQueryable.Select));

            ModifyCallExpression(methodInfo, selector);

            return this;
        }

        ITimeSeriesQueryable ITimeSeriesQueryable.Offset(TimeSpan offset)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.Offset));

            ModifyCallExpression(methodInfo, Expression.Constant(offset));

            return this;
        }

        ITimeSeriesAggregationQueryable ITimeSeriesAggregationQueryable.Offset(TimeSpan offset)
        {
            var methodInfo = typeof(ITimeSeriesAggregationQueryable)
                .GetMethod(nameof(ITimeSeriesAggregationQueryable.Offset));

            ModifyCallExpression(methodInfo, Expression.Constant(offset));

            return this;
        }

        ITimeSeriesQueryable ITimeSeriesQueryable.Scale(double value)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.Scale));

            ModifyCallExpression(methodInfo, Expression.Constant(value));

            return this;
        }

        ITimeSeriesAggregationQueryable ITimeSeriesAggregationQueryable.Scale(double value)
        {
            var methodInfo = typeof(ITimeSeriesAggregationQueryable)
                .GetMethod(nameof(ITimeSeriesAggregationQueryable.Scale));

            ModifyCallExpression(methodInfo, Expression.Constant(value));

            return this;
        }

        ITimeSeriesQueryBuilder ITimeSeriesQueryBuilder.FromFirst(Action<ITimePeriodBuilder> timePeriod)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.FromFirst));

            ModifyCallExpression(methodInfo, Expression.Constant(timePeriod));

            return this;
        }

        ITimeSeriesQueryBuilder ITimeSeriesQueryBuilder.FromLast(Action<ITimePeriodBuilder> timePeriod)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.FromLast));

            ModifyCallExpression(methodInfo, Expression.Constant(timePeriod));

            return this;
        }

        public TimeSeriesRawResult ToList()
        {
            var visitor = new TimeSeriesQueryVisitor<TEntity>(_documentQuery, _linqPathProvider);
            _query = visitor.Visit(_callExpression);

            return default;
        }

        TimeSeriesAggregationResult ITimeSeriesAggregationQueryable.ToList()
        {
            ToList();
            return default;
        }

        ITimeSeriesLoadQueryable<TEntity1> ITimeSeriesQueryable.LoadByTag<TEntity1>()
        {
            // never called
            throw new NotImplementedException();
        }

        public ITimeSeriesQueryable FromLast(Action<ITimePeriodBuilder> timePeriod)
        {
            throw new NotImplementedException();
        }

        public ITimeSeriesQueryable FromFirst(Action<ITimePeriodBuilder> timePeriod)
        {
            throw new NotImplementedException();
        }

        private void ModifyCallExpression(MethodInfo methodInfo, Expression arg = null)
        {
            Debug.Assert(_callExpression != null);
            Debug.Assert(methodInfo != null);

            _callExpression = arg == null 
                ? Expression.Call(_callExpression, methodInfo) 
                : Expression.Call(_callExpression, methodInfo, arg);
        }
    }
}
