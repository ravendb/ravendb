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
    public interface ITimeSeriesQueryBuilder : ITimeSeriesQueryable, ITimeSeriesAggregationQueryable
    {
        T Raw<T>(string queryText) where T : TimeSeriesQueryResult;

        ITimeSeriesQueryBuilder From(string name);

        ITimeSeriesQueryBuilder From(object documentInstance, string name);

        ITimeSeriesQueryBuilder Between(DateTime start, DateTime end);
    }

    internal class TimeSeriesQueryBuilder<TEntity> : ITimeSeriesQueryBuilder
    {
        private string _query;

        private string _name;
        private object _doc;

        private DateTime? _start;
        private DateTime? _end;

        private Expression _expression;
        private MethodCallExpression _callExpression;

        private IAbstractDocumentQuery<TEntity> _documentQuery;
        private LinqPathProvider _linqPathProvider;

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

        public ITimeSeriesQueryBuilder From(string name)
        {
            _name = name;

            var methodInfo = typeof(RavenQuery).GetMethods()
                .SingleOrDefault(method => 
                    method.Name == nameof(RavenQuery.TimeSeries) && 
                    method.IsGenericMethod == false && 
                    method.GetParameters().Length == 1);

            Debug.Assert(methodInfo != null);

            _callExpression = Expression.Call(
                methodInfo,
                Expression.Constant(_name)
            );

            /*_expression = RavenQuery.TimeSeries().
            _expression = Expression.Call(method: )*/
            return this;
        }

        public ITimeSeriesQueryBuilder From(object documentInstance, string name)
        {
            //todo check name not set

            _doc = documentInstance;
            _name = name;

            var methodInfo = typeof(RavenQuery).GetMethods()
                .SingleOrDefault(method =>
                    method.Name == nameof(RavenQuery.TimeSeries) &&
                    method.IsGenericMethod == false &&
                    method.GetParameters().Length == 2);

            Debug.Assert(methodInfo != null);

            _callExpression = Expression.Call(
                methodInfo,
                Expression.Constant(_doc),
                Expression.Constant(_name)
            );

            return this;
        }

        public ITimeSeriesQueryBuilder Between(DateTime start, DateTime end)
        {
            _start = start;
            _end = end;

            var methodInfo = typeof(RavenQuery).GetMethods()
                .SingleOrDefault(method =>
                    method.Name == nameof(RavenQuery.TimeSeries) &&
                    method.IsGenericMethod == false &&
                    method.GetParameters().Length == 4);


            Debug.Assert(methodInfo != null);

            _callExpression = Expression.Call(
                methodInfo,
                Expression.Constant(_doc),
                Expression.Constant(_name),
                Expression.Constant(_start),
                Expression.Constant(_end)
            );

            return this;
        }


        /*var query = session.Advanced.DocumentQuery<User>()
            .WhereGreaterThan(u => u.Age, 21)
            .SelectTimeSeries(builder => builder.From(name)
                .Between(start, end)
                .Where(ts => ts.Tag == "watches/fitbit")
                .GroupBy(g => g.Months(1)
                    .Select(x => new
                    {
                        Max = x.Max()
                    })))*/
        public ITimeSeriesQueryable Where(Expression<Func<TimeSeriesEntry, bool>> predicate)
        {
            ModifyCallExpression(predicate, typeof(ITimeSeriesQueryable).GetMethod(nameof(ITimeSeriesQueryable.Where)));
            return this;
        }

        private void ModifyCallExpression(Expression arg, MethodInfo methodInfo)
        {
            Debug.Assert(_callExpression != null);
            Debug.Assert(methodInfo != null);

            _callExpression = Expression.Call(_callExpression, methodInfo, arg);
        }

        public ITimeSeriesQueryable Offset(TimeSpan offset)
        {
            throw new NotImplementedException();
        }

        ITimeSeriesAggregationQueryable ITimeSeriesAggregationQueryable.Offset(TimeSpan offset)
        {
            throw new NotImplementedException();
        }

        public ITimeSeriesQueryable Scale(double value)
        {
            throw new NotImplementedException();
        }

        ITimeSeriesAggregationQueryable ITimeSeriesAggregationQueryable.Scale(double value)
        {
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

        public ITimeSeriesLoadQueryable<TEntity> LoadByTag<TEntity>()
        {
            throw new NotImplementedException();
        }

        public ITimeSeriesAggregationQueryable GroupBy(string s)
        {
            var methodInfo = typeof(ITimeSeriesQueryable)
                .GetMethod(nameof(ITimeSeriesQueryable.GroupBy), new []{ typeof(string) });
            ModifyCallExpression(Expression.Constant(s), methodInfo);
            return this;
        }

        public ITimeSeriesAggregationQueryable GroupBy(Action<ITimePeriodBuilder> timePeriod)
        {
            throw new NotImplementedException();
        }

        public ITimeSeriesAggregationQueryable Select(Expression<Func<ITimeSeriesGrouping, object>> selector)
        {
            throw new NotImplementedException();
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
    }

}
