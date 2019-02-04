using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.Highlighting;
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
    public partial class DocumentQuery<T> : AbstractDocumentQuery<T, DocumentQuery<T>>, IDocumentQuery<T>, IRawDocumentQuery<T>, IGraphQuery<T>, IDocumentQueryGenerator
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentQuery{T}"/> class.
        /// </summary>
        public DocumentQuery(InMemoryDocumentSessionOperations session, string indexName, string collectionName, bool isGroupBy, DeclareToken declareToken = null, List<LoadToken> loadTokens = null, string fromAlias = null)
            : base(session, indexName, collectionName, isGroupBy, declareToken, loadTokens, fromAlias)
        {
        }

        /// <inheritdoc />
        public IDocumentQuery<TProjection> SelectFields<TProjection>()
        {
            var propertyInfos = ReflectionUtil.GetPropertiesAndFieldsFor<TProjection>(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).ToList();
            var projections = propertyInfos.Select(x => x.Name).ToArray();
            var fields = propertyInfos.Select(p => p.Name).ToArray();
            return SelectFields<TProjection>(new QueryData(fields, projections));
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Distinct()
        {
            Distinct();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByScore()
        {
            OrderByScore();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByScoreDescending()
        {
            OrderByScoreDescending();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.IncludeExplanations(out Explanations explanations)
        {
            IncludeExplanations(null, out explanations);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.IncludeExplanations(ExplanationOptions options, out Explanations explanations)
        {
            IncludeExplanations(options, out explanations);
            return this;
        }

        /// <inheritdoc />
        public IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
        {
            return SelectFields<TProjection>(new QueryData(fields, fields));
        }

        /// <inheritdoc />
        public IDocumentQuery<TProjection> SelectFields<TProjection>(QueryData queryData)
        {
            return CreateDocumentQueryInternal<TProjection>(queryData);
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.AddParameter(string name, object value)
        {
            AddParameter(name, value);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.AddParameter(string name, object value)
        {
            AddParameter(name, value);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.AddParameter(string name, object value)
        {
            AddParameter(name, value);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan? waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AddOrder(string fieldName, bool descending, OrderingType ordering)
        {
            if (descending)
                OrderByDescending(fieldName, ordering);
            else
                OrderBy(fieldName, ordering);

            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending, OrderingType ordering)
        {
            var fieldName = GetMemberQueryPath(propertySelector.Body);
            if (descending)
                OrderByDescending(fieldName, ordering);
            else
                OrderBy(fieldName, ordering);

            return this;
        }

        void IQueryBase<T, IDocumentQuery<T>>.AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecuted(action);
        }

        void IQueryBase<T, IGraphQuery<T>>.AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecuted(action);
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted)
        {
            BeforeQueryExecuted(beforeQueryExecuted);
            return this;
        }

        void IQueryBase<T, IRawDocumentQuery<T>>.AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecuted(action);
        }

        void IQueryBase<T, IGraphQuery<T>>.AfterQueryExecuted(Action<QueryResult> action)
        {
            AfterQueryExecuted(action);
        }

        void IQueryBase<T, IDocumentQuery<T>>.AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecuted(action);
        }

        void IQueryBase<T, IRawDocumentQuery<T>>.AfterStreamExecuted(Action<BlittableJsonReaderObject> action)
        {
            AfterStreamExecuted(action);
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.OpenSubclause()
        {
            OpenSubclause();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.CloseSubclause()
        {
            CloseSubclause();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Search(string fieldName, string searchTerms, SearchOperator @operator)
        {
            Search(fieldName, searchTerms, @operator);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms, SearchOperator @operator)
        {
            Search(GetMemberQueryPath(propertySelector.Body), searchTerms, @operator);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Intersect()
        {
            Intersect();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.ContainsAll<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAny(fieldName, values);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.ContainsAny<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAll(fieldName, values);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        IGraphQuery<T> IQueryBase<T, IGraphQuery<T>>.Timings(out QueryTimings timings)
        {
            Timings(out timings);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.Timings(out QueryTimings timings)
        {
            IncludeTimings(out timings);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.Timings(out QueryTimings timings)
        {
            IncludeTimings(out timings);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Include(Expression<Func<T, object>> path)
        {
            Include(path);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Not
        {
            get
            {
                NegateNext();
                return this;
            }
        }

        /// <inheritdoc />
        public QueryResult GetQueryResult()
        {
            InitSync();

            return QueryOperation.CurrentQueryResults.CreateSnapshot();
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereLucene(string fieldName, string whereClause)
        {
            WhereLucene(fieldName, whereClause, exact: false);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereLucene(string fieldName, string whereClause, bool exact)
        {
            WhereLucene(fieldName, whereClause, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(string fieldName, object value, bool exact)
        {
            WhereEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(string fieldName, MethodCall value, bool exact)
        {
            WhereEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(WhereParams whereParams)
        {
            WhereEquals(whereParams);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereNotEquals(string fieldName, object value, bool exact)
        {
            WhereNotEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereNotEquals(string fieldName, MethodCall value, bool exact)
        {
            WhereNotEquals(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereNotEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact)
        {
            WhereNotEquals(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereNotEquals(WhereParams whereParams)
        {
            WhereNotEquals(whereParams);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values, bool exact)
        {
            WhereIn(fieldName, values, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values, bool exact)
        {
            WhereIn(GetMemberQueryPath(propertySelector.Body), values.Cast<object>(), exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereStartsWith(string fieldName, object value, bool exact)
        {
            WhereStartsWith(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEndsWith(string fieldName, object value, bool exact)
        {
            WhereEndsWith(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereBetween(string fieldName, object start, object end, bool exact)
        {
            WhereBetween(fieldName, start, end, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end, bool exact)
        {
            WhereBetween(GetMemberQueryPath(propertySelector.Body), start, end, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThan(string fieldName, object value, bool exact)
        {
            WhereGreaterThan(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereGreaterThan(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value, bool exact)
        {
            WhereGreaterThanOrEqual(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereGreaterThanOrEqual(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThan(string fieldName, object value, bool exact)
        {
            WhereLessThan(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereLessThan(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThanOrEqual(string fieldName, object value, bool exact)
        {
            WhereLessThanOrEqual(fieldName, value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact)
        {
            WhereLessThanOrEqual(GetMemberQueryPath(propertySelector.Body), value, exact);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereExists<TValue>(Expression<Func<T, TValue>> propertySelector)
        {
            WhereExists(GetMemberQueryPath(propertySelector.Body));
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereExists(string fieldName)
        {
            WhereExists(fieldName);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereRegex<TValue>(Expression<Func<T, TValue>> propertySelector, string pattern)
        {
            WhereRegex(GetMemberQueryPath(propertySelector.Body), pattern);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WhereRegex(string fieldName, string pattern)
        {
            WhereRegex(fieldName, pattern);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.AndAlso()
        {
            AndAlso();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.OrElse()
        {
            OrElse();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Boost(decimal boost)
        {
            Boost(boost);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Fuzzy(decimal fuzzy)
        {
            Fuzzy(fuzzy);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Proximity(int proximity)
        {
            Proximity(proximity);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

#if FEATURE_CUSTOM_SORTING
        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }
#endif

        /// <inheritdoc />
        IGroupByDocumentQuery<T> IDocumentQuery<T>.GroupBy(string fieldName, params string[] fieldNames)
        {
            GroupBy(fieldName, fieldNames);
            return new GroupByDocumentQuery<T>(this);
        }

        /// <inheritdoc />
        IGroupByDocumentQuery<T> IDocumentQuery<T>.GroupBy((string Name, GroupByMethod Method) field, params (string Name, GroupByMethod Method)[] fields)
        {
            GroupBy(field, fields);
            return new GroupByDocumentQuery<T>(this);
        }

        /// <inheritdoc />
        IDocumentQuery<TResult> IDocumentQuery<T>.OfType<TResult>()
        {
            return CreateDocumentQueryInternal<TResult>();
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy(string field, string sorterName)
        {
            OrderBy(field, sorterName);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy(string field, OrderingType ordering)
        {
            OrderBy(field, ordering);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector)
        {
            var rangeType = Conventions.GetRangeType(propertySelector.ReturnType);
            OrderBy(GetMemberQueryPathForOrderBy(propertySelector), OrderingUtil.GetOrderingFromRangeType(rangeType));
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName)
        {
            OrderBy(GetMemberQueryPathForOrderBy(propertySelector), sorterName);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering)
        {
            OrderBy(GetMemberQueryPathForOrderBy(propertySelector), ordering);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            foreach (var item in propertySelectors)
            {
                var rangeType = Conventions.GetRangeType(item.ReturnType);
                OrderBy(GetMemberQueryPathForOrderBy(item), OrderingUtil.GetOrderingFromRangeType(rangeType));
            }
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending(string field, string sorterName)
        {
            OrderByDescending(field, sorterName);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending(string field, OrderingType ordering)
        {
            OrderByDescending(field, ordering);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector)
        {
            var rangeType = Conventions.GetRangeType(propertySelector.ReturnType);
            OrderByDescending(GetMemberQueryPathForOrderBy(propertySelector), OrderingUtil.GetOrderingFromRangeType(rangeType));
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName)
        {
            OrderByDescending(GetMemberQueryPathForOrderBy(propertySelector), sorterName);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering)
        {
            OrderByDescending(GetMemberQueryPathForOrderBy(propertySelector), ordering);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            foreach (var item in propertySelectors)
            {
                var rangeType = Conventions.GetRangeType(item.ReturnType);
                OrderByDescending(GetMemberQueryPathForOrderBy(item), OrderingUtil.GetOrderingFromRangeType(rangeType));
            }

            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IQueryBase<T, IDocumentQuery<T>>.BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted)
        {
            BeforeQueryExecuted(beforeQueryExecuted);
            return this;
        }

        /// <inheritdoc />
        IRawDocumentQuery<T> IQueryBase<T, IRawDocumentQuery<T>>.BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted)
        {
            BeforeQueryExecuted(beforeQueryExecuted);
            return this;
        }

        public IGraphQuery<T> WithEdges(string alias, string edgeSelector, string query)
        {
            WithTokens.AddLast(new WithEdgesToken(alias, edgeSelector, query));
            return this;
        }

        public IGraphQuery<T> With<TOther>(string alias, string rawQuery)
        {
            return WithInternal(alias, (DocumentQuery<TOther>)Session.Advanced.RawQuery<TOther>(rawQuery));
        }

        public IGraphQuery<T> With<TOther>(string alias, IRavenQueryable<TOther> query)
        {
            var queryInspector = (RavenQueryInspector<TOther>)query;
            var docQuery = (DocumentQuery<TOther>)queryInspector.GetDocumentQuery(x => x.ParameterPrefix = $"w{WithTokens.Count}p");
            return WithInternal(alias, docQuery);
        }

        public IGraphQuery<T> With<TOther>(string alias, Func<IDocumentQueryBuilder, IDocumentQuery<TOther>> queryFactory)
        {
            var docQuery = (DocumentQuery<TOther>)queryFactory(new DocumentQueryBuilder(Session, $"w{WithTokens.Count}p"));
            return WithInternal(alias, docQuery);
        }

        private class DocumentQueryBuilder : IDocumentQueryBuilder
        {
            private readonly IDocumentSession _session;
            private readonly string _parameterPrefix;

            public DocumentQueryBuilder(IDocumentSession session, string parameterPrefix)
            {
                _session = session;
                _parameterPrefix = parameterPrefix;
            }

            public IDocumentQuery<T1> DocumentQuery<T1, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
            {
                var query = (DocumentQuery<T1>)_session.Advanced.DocumentQuery<T1, TIndexCreator>();
                query.ParameterPrefix = _parameterPrefix;
                return query;
            }

            public IDocumentQuery<T1> DocumentQuery<T1>(string indexName = null, string collectionName = null, bool isMapReduce = false)
            {
                var query = (DocumentQuery<T1>)_session.Advanced.DocumentQuery<T1>(indexName, collectionName, isMapReduce);
                query.ParameterPrefix = _parameterPrefix;
                return query;
            }
        }
        private IGraphQuery<T> WithInternal<TOther>(string alias, DocumentQuery<TOther> docQuery)
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
        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator()
        {
            return ExecuteQueryOperation(null).GetEnumerator();
        }

        /// <inheritdoc />
        T IDocumentQueryBase<T>.First()
        {
            return ExecuteQueryOperation(1).First();
        }

        /// <inheritdoc />
        T IDocumentQueryBase<T>.FirstOrDefault()
        {
            return ExecuteQueryOperation(1).FirstOrDefault();
        }

        /// <inheritdoc />
        T IDocumentQueryBase<T>.Single()
        {
            return ExecuteQueryOperation(2).Single();
        }

        /// <inheritdoc />
        T IDocumentQueryBase<T>.SingleOrDefault()
        {
            return ExecuteQueryOperation(2).SingleOrDefault();
        }

        /// <inheritdoc />
        bool IDocumentQueryBase<T>.Any()
        {
            if (IsDistinct)
            {
                // for distinct it is cheaper to do count 1
                return ExecuteQueryOperation(1).Any();
            }

            Take(0);
            var queryResult = GetQueryResult();
            return queryResult.TotalResults > 0;
        }

        private List<T> ExecuteQueryOperation(int? take)
        {
            ExecuteQueryOperationInternal(take);

            return QueryOperation.Complete<T>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ExecuteQueryOperationInternal(int? take)
        {
            if (take.HasValue && (PageSize.HasValue == false || PageSize > take))
                Take(take.Value);

            InitSync();
        }

        /// <inheritdoc />
        int IDocumentQueryBase<T>.Count()
        {
            Take(0);
            var queryResult = GetQueryResult();
            return queryResult.TotalResults;
        }

        /// <inheritdoc />
        Lazy<int> IDocumentQueryBase<T>.CountLazily()
        {
            if (QueryOperation == null)
            {
                Take(0);
                QueryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(TheSession.Conventions, QueryOperation, AfterQueryExecutedCallback);

            return ((DocumentSession)TheSession).AddLazyCountOperation(lazyQueryOperation);
        }

        /// <inheritdoc />
        Lazy<IEnumerable<T>> IDocumentQueryBase<T>.Lazily(Action<IEnumerable<T>> onEval)
        {
            if (QueryOperation == null)
            {
                QueryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(TheSession.Conventions, QueryOperation, AfterQueryExecutedCallback);
            return ((DocumentSession)TheSession).AddLazyOperation(lazyQueryOperation, onEval);
        }

        protected void InitSync()
        {
            if (QueryOperation != null)
                return;

            var beforeQueryExecutedEventArgs = new BeforeQueryEventArgs(TheSession, this);
            TheSession.OnBeforeQueryInvoke(beforeQueryExecutedEventArgs);

            QueryOperation = InitializeQueryOperation();
            ExecuteActualQuery();
        }

        private void ExecuteActualQuery()
        {
            using (QueryOperation.EnterQueryContext())
            {
                var command = QueryOperation.CreateRequest();
                TheSession.RequestExecutor.Execute(command, TheSession.Context, sessionInfo: TheSession.SessionInfo);
                QueryOperation.SetResult(command.Result);
            }

            InvokeAfterQueryExecuted(QueryOperation.CurrentQueryResults);
        }

        private DocumentQuery<TResult> CreateDocumentQueryInternal<TResult>(QueryData queryData = null)
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

            var query = new DocumentQuery<TResult>(
                TheSession,
                IndexName,
                CollectionName,
                IsGroupBy,
                queryData?.DeclareToken,
                queryData?.LoadTokens,
                queryData?.FromAlias)
            {
                QueryRaw = QueryRaw,
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
                RootTypes = { typeof(T) },
                BeforeQueryExecutedCallback = BeforeQueryExecutedCallback,
                AfterQueryExecutedCallback = AfterQueryExecutedCallback,
                AfterStreamExecutedCallback = AfterStreamExecutedCallback,
                HighlightingTokens = HighlightingTokens,
                QueryHighlightings = QueryHighlightings,
                DisableEntitiesTracking = DisableEntitiesTracking,
                DisableCaching = DisableCaching,
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
            return ((IDocumentQueryGenerator)Session).CreateRavenQueryInspector<TS>();
        }

        public IDocumentQuery<TResult> Query<TResult>(string indexName, string collectionName, bool isMapReduce)
        {
            if (indexName != IndexName || collectionName != CollectionName)
                throw new InvalidOperationException(
                    $"DocumentQuery source has (index name: {IndexName}, collection: {CollectionName}), but got request for (index name: {indexName}, collection: {collectionName}), you cannot change the index name / collection when using DocumentQuery as the source");

            return CreateDocumentQueryInternal<TResult>();
        }

        public IAsyncDocumentQuery<TResult> AsyncQuery<TResult>(string indexName, string collectionName, bool isMapReduce)
        {
            throw new NotSupportedException("Cannot create an async LINQ query from DocumentQuery, you need to use AsyncDocumentQuery for that");
        }
    }
}
