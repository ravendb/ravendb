using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// A query against a Raven index
    /// </summary>
    public partial class AsyncDocumentQuery<T> : AbstractDocumentQuery<T, AsyncDocumentQuery<T>>, IAbstractDocumentQueryImpl<T>,
        IAsyncRawDocumentQuery<T>, IAsyncGraphQuery<T>, IDocumentQueryGenerator, IAsyncDocumentQuery<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentQuery{T}"/> class.
        /// </summary>
        public AsyncDocumentQuery(InMemoryDocumentSessionOperations session, string indexName, string collectionName, bool isGroupBy, IEnumerable<DeclareToken> declareTokens = null, List<LoadToken> loadTokens = null, string fromAlias = null, bool? isProjectInfo = false)
            : base(session, indexName, collectionName, isGroupBy, declareTokens, loadTokens, fromAlias, isProjectInfo)
        {
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Include(Expression<Func<T, object>> path)
        {
            Include(path);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Not
        {
            get
            {
                NegateNext();
                return this;
            }
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.AddParameter(string name, object value)
        {
            AddParameter(name, value);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.Timings(out QueryTimings timings)
        {
            Timings(out timings);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLucene(string fieldName, string whereClause)
        {
            WhereLucene(fieldName, whereClause, exact: false);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLucene(string fieldName, string whereClause, bool exact)
        {
            WhereLucene(fieldName, whereClause, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(string fieldName, object value, bool exact)
        {
            WhereEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(string fieldName, MethodCall value, bool exact)
        {
            WhereEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(WhereParams whereParams)
        {
            WhereEquals(whereParams);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereNotEquals(string fieldName, object value, bool exact)
        {
            WhereNotEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereNotEquals(string fieldName, MethodCall value, bool exact)
        {
            WhereNotEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereNotEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact)
        {
            WhereNotEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereNotEquals(WhereParams whereParams)
        {
            WhereNotEquals(whereParams);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values, bool exact)
        {
            WhereIn(fieldName, values, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values, bool exact)
        {
            WhereIn(GetMemberQueryPath(propertySelector.Body), values.Cast<object>(), exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereStartsWith(string fieldName, object value)
        {
            WhereStartsWith(fieldName, value, exact: false);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereStartsWith(string fieldName, object value, bool exact)
        {
            WhereStartsWith(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value, exact: false);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEndsWith(string fieldName, object value)
        {
            WhereEndsWith(fieldName, value, exact: false);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEndsWith(string fieldName, object value, bool exact)
        {
            WhereEndsWith(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value, exact: false);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereBetween(string fieldName, object start, object end, bool exact)
        {
            WhereBetween(fieldName, start, end, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end, bool exact)
        {
            WhereBetween(GetMemberQueryPath(propertySelector.Body), start, end, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThan(string fieldName, object value, bool exact)
        {
            WhereGreaterThan(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereGreaterThan(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value, bool exact)
        {
            WhereGreaterThanOrEqual(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereGreaterThanOrEqual(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThan(string fieldName, object value, bool exact)
        {
            WhereLessThan(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereLessThan(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThanOrEqual(string fieldName, object value, bool exact)
        {
            WhereLessThanOrEqual(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereLessThanOrEqual(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereExists<TValue>(Expression<Func<T, TValue>> propertySelector)
        {
            WhereExists(GetMemberQueryPath(propertySelector.Body));
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereExists(string fieldName)
        {
            WhereExists(fieldName);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereRegex<TValue>(Expression<Func<T, TValue>> propertySelector, string pattern)
        {
            WhereRegex(GetMemberQueryPath(propertySelector.Body), pattern);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereRegex(string fieldName, string pattern)
        {
            WhereRegex(fieldName, pattern);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AndAlso()
        {
            AndAlso();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrElse()
        {
            OrElse();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Boost(decimal boost)
        {
            Boost(boost);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Fuzzy(decimal fuzzy)
        {
            Fuzzy(fuzzy);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Proximity(int proximity)
        {
            Proximity(proximity);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

#if FEATURE_CUSTOM_SORTING
        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }
#endif

        /// <inheritdoc />
        IAsyncDocumentQuery<TResult> IAsyncDocumentQuery<T>.OfType<TResult>()
        {
            return CreateDocumentQueryInternal<TResult>();
        }

        /// <inheritdoc />
        IAsyncGroupByDocumentQuery<T> IAsyncDocumentQuery<T>.GroupBy(string fieldName, params string[] fieldNames)
        {
            GroupBy(fieldName, fieldNames);
            return new AsyncGroupByDocumentQuery<T>(this);
        }

        /// <inheritdoc />
        IAsyncGroupByDocumentQuery<T> IAsyncDocumentQuery<T>.GroupBy((string Name, GroupByMethod Method) field, params (string Name, GroupByMethod Method)[] fields)
        {
            GroupBy(field, fields);
            return new AsyncGroupByDocumentQuery<T>(this);
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy(string field, string sorterName)
        {
            OrderBy(field, sorterName);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy(string field, OrderingType ordering)
        {
            OrderBy(field, ordering);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector)
        {
            var rangeType = Conventions.GetRangeType(propertySelector.ReturnType);
            OrderBy(GetMemberQueryPathForOrderBy(propertySelector), OrderingUtil.GetOrderingFromRangeType(rangeType));
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName)
        {
            OrderBy(GetMemberQueryPathForOrderBy(propertySelector), sorterName);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering)
        {
            OrderBy(GetMemberQueryPathForOrderBy(propertySelector), ordering);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            foreach (var item in propertySelectors)
            {
                var rangeType = Conventions.GetRangeType(item.ReturnType);
                OrderBy(GetMemberQueryPathForOrderBy(item), OrderingUtil.GetOrderingFromRangeType(rangeType));
            }

            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending(string field, string sorterName)
        {
            OrderByDescending(field, sorterName);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending(string field, OrderingType ordering)
        {
            OrderByDescending(field, ordering);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector)
        {
            var rangeType = Conventions.GetRangeType(propertySelector.ReturnType);
            OrderByDescending(GetMemberQueryPathForOrderBy(propertySelector), OrderingUtil.GetOrderingFromRangeType(rangeType));
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName)
        {
            OrderByDescending(GetMemberQueryPathForOrderBy(propertySelector), sorterName);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering)
        {
            OrderByDescending(GetMemberQueryPathForOrderBy(propertySelector), ordering);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            foreach (var item in propertySelectors)
            {
                var rangeType = Conventions.GetRangeType(item.ReturnType);
                OrderByDescending(GetMemberQueryPathForOrderBy(item), OrderingUtil.GetOrderingFromRangeType(rangeType));
            }

            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecuted(action);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted)
        {
            BeforeQueryExecuted(beforeQueryExecuted);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted)
        {
            BeforeQueryExecuted(beforeQueryExecuted);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted)
        {
            BeforeQueryExecuted(beforeQueryExecuted);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.AddParameter(string name, object value)
        {
            AddParameter(name, value);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.AddParameter(string name, object value)
        {
            AddParameter(name, value);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>()
        {
            return SelectFields<TProjection>(Queries.ProjectionBehavior.Default);
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(ProjectionBehavior projectionBehavior)
        {
            var propertyInfos = ReflectionUtil.GetPropertiesAndFieldsFor<TProjection>(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).ToList();
            var projections = propertyInfos.Select(x => x.Name).ToArray();
            var fields = propertyInfos.Select(p => p.Name).ToArray();
            return SelectFields<TProjection>(new QueryData(fields, projections)
            {
                IsProjectInto = true,
                ProjectionBehavior = projectionBehavior
            });
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
        {
            return SelectFields<TProjection>(Queries.ProjectionBehavior.Default, fields);
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(ProjectionBehavior projectionBehavior, params string[] fields)
        {
            return SelectFields<TProjection>(new QueryData(fields, fields)
            {
                IsProjectInto = true,
                ProjectionBehavior = projectionBehavior
            });
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(QueryData queryData)
        {
            queryData.IsProjectInto = true;
            return CreateDocumentQueryInternal<TProjection>(queryData);
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<TTimeSeries> IAsyncDocumentQuery<T>.SelectTimeSeries<TTimeSeries>(Func<ITimeSeriesQueryBuilder, TTimeSeries> timeSeriesQuery)
        {
            var queryData = CreateTimeSeriesQueryData(timeSeriesQuery);
            return SelectFields<TTimeSeries>(queryData);
        }

        /// <inheritdoc />
        Lazy<Task<int>> IAsyncDocumentQueryBase<T>.CountLazilyAsync(CancellationToken token)
        {
            if (QueryOperation == null)
            {
                Take(0);
                QueryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(TheSession, QueryOperation, AfterQueryExecutedCallback);

            return ((AsyncDocumentSession)TheSession).AddLazyCountOperation(lazyQueryOperation, token);
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AddOrder(string fieldName, bool descending, OrderingType ordering)
        {
            if (descending)
                OrderByDescending(fieldName, ordering);
            else
                OrderBy(fieldName, ordering);

            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByScore()
        {
            OrderByScore();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByScoreDescending()
        {
            OrderByScoreDescending();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending, OrderingType ordering)
        {
            var fieldName = GetMemberQueryPath(propertySelector.Body);
            if (descending)
                OrderByDescending(fieldName, ordering);
            else
                OrderBy(fieldName, ordering);

            return this;
        }

        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecuted(action);
            return this;
        }

        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecuted(action);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecuted(action);
            return this;
        }

        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecuted(action);
            return this;
        }

        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecuted(action);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OpenSubclause()
        {
            OpenSubclause();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Search(string fieldName, string searchTerms, SearchOperator @operator)
        {
            Search(fieldName, searchTerms, @operator);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms, SearchOperator @operator)
        {
            Search(GetMemberQueryPath(propertySelector.Body), searchTerms, @operator);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.CloseSubclause()
        {
            CloseSubclause();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.NegateNext()
        {
            NegateNext();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Intersect()
        {
            Intersect();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAll<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAny(fieldName, values);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAny<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAll(fieldName, values);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        IAsyncGraphQuery<T> IQueryBase<T, IAsyncGraphQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IQueryBase<T, IAsyncDocumentQuery<T>>.Timings(out QueryTimings timings)
        {
            IncludeTimings(out timings);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IQueryBase<T, IAsyncRawDocumentQuery<T>>.Timings(out QueryTimings timings)
        {
            IncludeTimings(out timings);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Distinct()
        {
            Distinct();
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.IncludeExplanations(out Explanations explanations)
        {
            IncludeExplanations(null, out explanations);
            return this;
        }

        /// <inheritdoc />
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.IncludeExplanations(ExplanationOptions options, out Explanations explanations)
        {
            IncludeExplanations(options, out explanations);
            return this;
        }

        /// <inheritdoc />
        IAsyncRawDocumentQuery<T> IAsyncRawDocumentQuery<T>.Projection(ProjectionBehavior projectionBehavior)
        {
            ProjectionBehavior = projectionBehavior;
            return this;
        }

        /// <inheritdoc />
        Task<List<T>> IAsyncDocumentQueryBase<T>.ToListAsync(CancellationToken token)
        {
            return ExecuteQueryOperation(null, token);
        }

        /// <inheritdoc />
        Task<T[]> IAsyncDocumentQueryBase<T>.ToArrayAsync(CancellationToken token)
        {
            return ExecuteQueryOperationAsArray(null, token);
        }

        /// <inheritdoc />
        async Task<T> IAsyncDocumentQueryBase<T>.FirstAsync(CancellationToken token)
        {
            var operation = await ExecuteQueryOperation(1, token).ConfigureAwait(false);
            return operation.First();
        }

        /// <inheritdoc />
        async Task<T> IAsyncDocumentQueryBase<T>.FirstOrDefaultAsync(CancellationToken token)
        {
            var operation = await ExecuteQueryOperation(1, token).ConfigureAwait(false);
            return operation.FirstOrDefault();
        }

        /// <inheritdoc />
        async Task<T> IAsyncDocumentQueryBase<T>.SingleAsync(CancellationToken token)
        {
            var operation = await ExecuteQueryOperation(2, token).ConfigureAwait(false);
            return operation.Single();
        }

        /// <inheritdoc />
        async Task<T> IAsyncDocumentQueryBase<T>.SingleOrDefaultAsync(CancellationToken token)
        {
            var operation = await ExecuteQueryOperation(2, token).ConfigureAwait(false);
            return operation.SingleOrDefault();
        }

        /// <inheritdoc />
        async Task<bool> IAsyncDocumentQueryBase<T>.AnyAsync(CancellationToken token)
        {
            if (IsDistinct)
            {
                // for distinct it is cheaper to do count 1
                var operation = await ExecuteQueryOperation(1, token).ConfigureAwait(false);
                return operation.Any();
            }

            Take(0);
            var result = await GetQueryResultAsync(token).ConfigureAwait(false);
            return result.TotalResults > 0;
        }

        private async Task<List<T>> ExecuteQueryOperation(int? take, CancellationToken token)
        {
            await ExecuteQueryOperationInternal(take, token).ConfigureAwait(false);

            return QueryOperation.Complete<T>();
        }

        private async Task<T[]> ExecuteQueryOperationAsArray(int? take, CancellationToken token)
        {
            await ExecuteQueryOperationInternal(take, token).ConfigureAwait(false);

            return QueryOperation.CompleteAsArray<T>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task ExecuteQueryOperationInternal(int? take, CancellationToken token)
        {
            if (take.HasValue && (PageSize.HasValue == false || PageSize > take))
                Take(take.Value);

            await InitAsync(token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        Lazy<Task<IEnumerable<T>>> IAsyncDocumentQueryBase<T>.LazilyAsync(Action<IEnumerable<T>> onEval)
        {
            var lazyQueryOperation = GetLazyQueryOperation();
            return ((AsyncDocumentSession)TheSession).AddLazyOperation(lazyQueryOperation, onEval);
        }

        /// <inheritdoc />
        async Task<int> IAsyncDocumentQueryBase<T>.CountAsync(CancellationToken token)
        {
            Take(0);
            var result = await GetQueryResultAsync(token).ConfigureAwait(false);
            return result.TotalResults;
        }

        /// <inheritdoc />
        async Task<long> IAsyncDocumentQueryBase<T>.LongCountAsync(CancellationToken token)
        {
            Take(0);
            var result = await GetQueryResultAsync(token).ConfigureAwait(false);
            return result.LongTotalResults;
        }

        /// <inheritdoc />
        public async Task<QueryResult> GetQueryResultAsync(CancellationToken token = default(CancellationToken))
        {
            await InitAsync(token).ConfigureAwait(false);

            return QueryOperation.CurrentQueryResults.CreateSnapshot();
        }

        protected async Task InitAsync(CancellationToken token)
        {
            if (QueryOperation != null)
                return;

            QueryOperation = InitializeQueryOperation();
            await ExecuteActualQueryAsync(token).ConfigureAwait(false);
        }

        private async Task ExecuteActualQueryAsync(CancellationToken token)
        {
            using (TheSession.AsyncTaskHolder())
            {
                using (QueryOperation.EnterQueryContext())
                {
                    var command = QueryOperation.CreateRequest();
                    await TheSession.RequestExecutor.ExecuteAsync(command, TheSession.Context, TheSession._sessionInfo, token).ConfigureAwait(false);
                    QueryOperation.SetResult(command.Result);
                }

                InvokeAfterQueryExecuted(QueryOperation.CurrentQueryResults);
            }
        }

        internal AsyncDocumentQuery<TResult> CreateDocumentQueryInternal<TResult>(QueryData queryData = null)
        {
            FieldsToFetchToken newFieldsToFetch;
            if (queryData != null && queryData.Fields.Length > 0)
            {
                var fields = queryData.Fields;
                if (IsGroupBy == false)
                {
                    var identityProperty = Conventions.GetIdentityProperty(typeof(TResult));
                    if (identityProperty != null)
                        fields = queryData.Fields
                            .Select(x => x == identityProperty.Name ? Constants.Documents.Indexing.Fields.DocumentIdFieldName : x)
                            .ToArray();
                }

                GetSourceAliasIfExists(queryData, fields, out var sourceAlias);

                newFieldsToFetch = FieldsToFetchToken.Create(fields, queryData.Projections.ToArray(), queryData.IsCustomFunction, sourceAlias);
            }
            else
                newFieldsToFetch = null;

            if (newFieldsToFetch != null)
                UpdateFieldsToFetchToken(newFieldsToFetch);

            var query = new AsyncDocumentQuery<TResult>(
                TheSession,
                IndexName,
                CollectionName,
                IsGroupBy,
                queryData?.DeclareTokens,
                queryData?.LoadTokens,
                queryData?.FromAlias,
                queryData?.IsProjectInto)
            {
                PageSize = PageSize,
                SelectTokens = new LinkedList<QueryToken>(SelectTokens),
                FieldsToFetchToken = FieldsToFetchToken,
                WhereTokens = new LinkedList<QueryToken>(WhereTokens),
                OrderByTokens = new LinkedList<QueryToken>(OrderByTokens),
                GroupByTokens = new LinkedList<QueryToken>(GroupByTokens),
                QueryParameters = new Parameters(QueryParameters),
                Start = Start,
                Timeout = Timeout,
                QueryStats = QueryStats,
                TheWaitForNonStaleResults = TheWaitForNonStaleResults,
                Negate = Negate,
                DocumentIncludes = new HashSet<string>(DocumentIncludes),
                CounterIncludesTokens = CounterIncludesTokens,
                TimeSeriesIncludesTokens = TimeSeriesIncludesTokens,
                CompareExchangeValueIncludesTokens = CompareExchangeValueIncludesTokens,
                RootTypes = { typeof(T) },
                BeforeQueryExecutedCallback = BeforeQueryExecutedCallback,
                AfterQueryExecutedCallback = AfterQueryExecutedCallback,
                AfterStreamExecutedCallback = AfterStreamExecutedCallback,
                HighlightingTokens = HighlightingTokens,
                QueryHighlightings = QueryHighlightings,
                DisableEntitiesTracking = DisableEntitiesTracking,
                DisableCaching = DisableCaching,
                ProjectionBehavior = queryData?.ProjectionBehavior ?? ProjectionBehavior,
                QueryTimings = QueryTimings,
                Explanations = Explanations,
                ExplanationToken = ExplanationToken,
                IsIntersect = IsIntersect,
                DefaultOperator = DefaultOperator
            };

            return query;
        }

        public IRavenQueryable<T> ToQueryable()
        {
            var type = typeof(T);

            var queryStatistics = new QueryStatistics();
            var highlightings = new LinqQueryHighlightings();

            var ravenQueryInspector = new RavenQueryInspector<T>();
            var ravenQueryProvider = new RavenQueryProvider<T>(
                CreateDocumentQueryInternal<T>(), // clone
                IndexName,
                CollectionName,
                type,
                queryStatistics,
                highlightings,
                IsGroupBy,
                Conventions);

            ravenQueryInspector.Init(ravenQueryProvider,
                queryStatistics,
                highlightings,
                IndexName,
                CollectionName,
                null,
                TheSession,
                IsGroupBy);

            return ravenQueryInspector;
        }

        InMemoryDocumentSessionOperations IDocumentQueryGenerator.Session => TheSession;

        RavenQueryInspector<TS> IDocumentQueryGenerator.CreateRavenQueryInspector<TS>()
        {
            return ((IDocumentQueryGenerator)AsyncSession).CreateRavenQueryInspector<TS>();
        }

        public IDocumentQuery<TResult> Query<TResult>(string indexName, string collectionName, bool isMapReduce)
        {
            throw new NotSupportedException("Cannot create an sync LINQ query from AsyncDocumentQuery, you need to use DocumentQuery for that");
        }

        public IAsyncDocumentQuery<TResult> AsyncQuery<TResult>(string indexName, string collectionName, bool isMapReduce)
        {
            if (indexName != IndexName || collectionName != CollectionName)
                throw new InvalidOperationException(
                    $"AsyncDocumentQuery source has (index name: {IndexName}, collection: {CollectionName}), but got request for (index name: {indexName}, collection: {collectionName}), you cannot change the index name / collection when using AsyncDocumentQuery as the source");

            return CreateDocumentQueryInternal<TResult>();
        }

        public IAsyncGraphQuery<T> With<TOther>(string alias, string rawQuery)
        {
            return WithInternal(alias, (AsyncDocumentQuery<TOther>)AsyncSession.Advanced.AsyncRawQuery<TOther>(rawQuery));
        }

        public IAsyncGraphQuery<T> WithEdges(string alias, string edgeSelector, string query)
        {
            WithTokens.AddLast(new WithEdgesToken(alias, edgeSelector, query));
            return this;
        }

        public IAsyncGraphQuery<T> With<TOther>(string alias, IRavenQueryable<TOther> query)
        {
            var queryInspector = (RavenQueryInspector<TOther>)query;
            var docQuery = (AsyncDocumentQuery<TOther>)queryInspector.GetAsyncDocumentQuery(x => x.ParameterPrefix = $"w{WithTokens.Count}p");
            return WithInternal(alias, docQuery);
        }

        public IAsyncGraphQuery<T> With<TOther>(string alias, Func<IAsyncDocumentQueryBuilder, IAsyncDocumentQuery<TOther>> queryFactory)
        {
            var docQuery = (AsyncDocumentQuery<TOther>)queryFactory(new AsyncDocumentQueryBuilder(AsyncSession, $"w{WithTokens.Count}p"));
            return WithInternal(alias, docQuery);
        }

        private class AsyncDocumentQueryBuilder : IAsyncDocumentQueryBuilder
        {
            private readonly IAsyncDocumentSession _session;
            private readonly string _parameterPrefix;

            public AsyncDocumentQueryBuilder(IAsyncDocumentSession session, string parameterPrefix)
            {
                _session = session;
                _parameterPrefix = parameterPrefix;
            }

            public IAsyncDocumentQuery<T1> AsyncDocumentQuery<T1, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new()
            {
                var query = (AsyncDocumentQuery<T1>)_session.Advanced.AsyncDocumentQuery<T1, TIndexCreator>();
                query.ParameterPrefix = _parameterPrefix;
                return query;
            }

            public IAsyncDocumentQuery<T1> AsyncDocumentQuery<T1>(string indexName = null, string collectionName = null, bool isMapReduce = false)
            {
                var query = (AsyncDocumentQuery<T1>)_session.Advanced.AsyncDocumentQuery<T1>(indexName, collectionName, isMapReduce);
                query.ParameterPrefix = _parameterPrefix;
                return query;
            }
        }

        private IAsyncGraphQuery<T> WithInternal<TOther>(string alias, AsyncDocumentQuery<TOther> docQuery)
        {
            if (docQuery.SelectTokens?.Count > 0)
            {
                throw new NotSupportedException($"Select is not permitted in a 'With' clause in query:{docQuery}");
            }

            foreach (var keyValue in docQuery.QueryParameters)
            {
                QueryParameters.Add(keyValue.Key, keyValue.Value);
            }

            WithTokens.AddLast(new WithToken(alias, docQuery.ToString()));

            if (docQuery.TheWaitForNonStaleResults)
            {
                TheWaitForNonStaleResults = true;
                if (Timeout == null || Timeout < docQuery.Timeout)
                {
                    Timeout = docQuery.Timeout;
                }
            }

            return this;
        }
    }
}
