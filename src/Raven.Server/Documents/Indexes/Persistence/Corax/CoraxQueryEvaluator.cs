using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using BinaryExpression = Raven.Server.Documents.Queries.AST.BinaryExpression;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxQueryEvaluator : IDisposable
    {
        private readonly Index _index;
        private readonly IndexSearcher _searcher;
        private readonly ByteStringContext _allocator;
        private IndexQueryServerSide _query;
        private const int TakeAll = -1;
        private const int ScoreId = -1;
        internal Dictionary<string, object> QueryData;


        [CanBeNull]
        private FieldsToFetch _fieldsToFetch;

        public CoraxQueryEvaluator(Index index, IndexSearcher searcher)
        {
            _allocator = new(SharedMultipleUseFlag.None);
            _searcher = searcher;
            _index = index;
            QueryData = new();
        }

        public IQueryMatch Search(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Dictionary<string, object> queryData, int take = TakeAll)
        {
            _fieldsToFetch = fieldsToFetch;
            _query = query;
            QueryData = queryData;

            var match = _query.Metadata.Query.Where is null
                ? _searcher.AllEntries()
                : Evaluate(query.Metadata.Query.Where, false, take, default(NullScoreFunction));

            if (query.Metadata.OrderBy is not null)
                match = OrderBy(match, query.Metadata.Query.OrderBy, take);

            return match;
        }

        private IQueryMatch Evaluate<TScoreFunction>(QueryExpression condition, bool isNegated, int take, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            switch (condition)
            {
                case NegatedExpression negatedExpression:
                    isNegated = !isNegated;
                    return Evaluate(negatedExpression.Expression, isNegated, take, scoreFunction);
                case TrueExpression:
                case null:
                    return _searcher.AllEntries();
                case BetweenExpression betweenExpression:
                    return EvaluateBetween(betweenExpression, isNegated, take, scoreFunction);
                case MethodExpression methodExpression:
                    var expressionType = QueryMethod.GetMethodType(methodExpression.Name.Value);
                    int fieldId;
                    switch (expressionType)
                    {
                        case MethodType.StartsWith:
                            var (fieldName, fieldValue) = GetQueryData(methodExpression.Arguments[0], methodExpression.Arguments[1] as ValueExpression);
                            fieldName = GetField(methodExpression.Arguments[0]);
                            fieldId = GetFieldIdInIndex(fieldName);
                            
                            if (fieldValue is Client.Constants.Documents.Indexing.Fields.NullValue)
                                throw new InvalidQueryException("Method startsWith() expects to get an argument of type String while it got Null");

                            return _searcher.StartWithQuery(fieldName, fieldValue.ToString(), scoreFunction, isNegated, fieldId);
                        case MethodType.EndsWith:
                            (fieldName, fieldValue) = GetQueryData(methodExpression.Arguments[0], methodExpression.Arguments[1] as ValueExpression);
                            fieldId = GetFieldIdInIndex(fieldName);
                            return _searcher.EndsWithQuery(fieldName, fieldValue.ToString(), scoreFunction, isNegated,
                                fieldId);
                        case MethodType.Exact:
                            return BinaryEvaluator((BinaryExpression)methodExpression.Arguments[0], isNegated, take, scoreFunction);
                        case MethodType.Boost:
                            var boost = methodExpression.Arguments[1] as ValueExpression;
                            if (float.TryParse(boost?.Token.Value, out var constantValue) == false)
                                throw new InvalidQueryException("Invalid boost value.");

                            return Evaluate(methodExpression.Arguments[0], isNegated, TakeAll, new ConstantScoreFunction(constantValue));
                        case MethodType.Search:
                            return SearchMethod(methodExpression, isNegated, scoreFunction);
                        case MethodType.Exists:
                            fieldName = GetField(methodExpression.Arguments[0]);
                            return _searcher.ExistsQuery(fieldName);
                        default:
                            throw new NotImplementedException($"Method {expressionType} is not implemented.");
                    }

                case InExpression inExpression:
                    return (inExpression.Source, inExpression.Values) switch
                    {
                        (FieldExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list, scoreFunction),
                        _ => throw new NotSupportedException("InExpression.")
                    };

                case BinaryExpression be:
                    return BinaryEvaluator(be, isNegated, take, scoreFunction);
            }

            throw new EvaluateException($"Evaluation failed in {nameof(CoraxQueryEvaluator)}.");
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IQueryMatch SearchMethod<TScoreFunction>(MethodExpression expression, bool isNegated, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var fieldName = _index.Type is IndexType.AutoMap or IndexType.AutoMapReduce
                ? $"search({GetField(expression.Arguments[0])})"
                : GetField(expression.Arguments[0]);
            var fieldId = GetFieldIdInIndex(fieldName);
            Constants.Search.Operator @operator = Constants.Search.Operator.Or;


            if (expression.Arguments.Count is < 2 or > 3)
                throw new InvalidQueryException($"Invalid amount of parameter in {nameof(MethodType.Search)}.");

            if (expression.Arguments[1] is not ValueExpression searchParam)
                throw new InvalidQueryException($"You need to pass value in second argument of {nameof(MethodType.Search)}.");

            //STRUCTURE: <NAME><SEARCH_VALUE><OPERATOR>
            string searchTerm = searchParam.GetValue(_query.QueryParameters).ToString();
            AddTermToQueryData(GetField(expression.Arguments[0]), searchTerm);
            
            
            if (expression.Arguments.Count is not 3)
                return _searcher.SearchQuery(fieldName, searchTerm, @operator, isNegated, fieldId);


            if (expression.Arguments[2] is not FieldExpression operatorType)
                throw new InvalidQueryException($"Expected AND or OR in third argument of {nameof(MethodType.Search)}.");

            @operator = operatorType.FieldValue.ToLowerInvariant() switch
            {
                "and" => Constants.Search.Operator.And,
                "or" => Constants.Search.Operator.Or,
                _ => throw new InvalidQueryException($"Expected AND or OR in third argument of {nameof(MethodType.Search)}.")
            };


            return _searcher.SearchQuery<TScoreFunction>(fieldName, searchTerm, scoreFunction, @operator, fieldId, isNegated);
        }

        [SkipLocalsInit]
        private IQueryMatch BinaryEvaluator<TScoreFunction>(BinaryExpression expression, bool isNegated, int take, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (isNegated == true)
                expression.Operator = GetNegated(expression.Operator);

            string fieldName;
            object fieldValue;

            switch (expression.Operator)
            {
                case OperatorType.Or:
                    return _searcher.Or(Evaluate(expression.Left, isNegated, take, scoreFunction), Evaluate(expression.Right, isNegated, take, scoreFunction));
                case OperatorType.Equal:
                {
                    (fieldName, fieldValue) = GetQueryData(expression.Left, (ValueExpression)expression.Right);
                    var match = _searcher.TermQuery(fieldName, fieldValue.ToString(), GetFieldIdInIndex(fieldName));
                    return scoreFunction is NullScoreFunction
                        ? match
                        : _searcher.Boost(match, scoreFunction);
                }
                case OperatorType.NotEqual:
                    (fieldName, fieldValue) = GetQueryData(expression.Left, (ValueExpression)expression.Right);
                    return isNegated
                        ? _searcher.TermQuery(fieldName, fieldValue.ToString(), GetFieldIdInIndex(fieldName))
                        : _searcher.UnaryQuery(_searcher.AllEntries(), GetFieldIdInIndex(fieldName), fieldValue.ToString(), UnaryMatchOperation.NotEquals);
            }

            if (expression.IsRangeOperation)
                return EvaluateUnary(expression.Operator, _searcher.AllEntries(), expression, isNegated, take, scoreFunction);

            var left = expression.Left as BinaryExpression;
            var isLeftUnary = IsUnary(left);
            var right = expression.Right as BinaryExpression;
            var isRightUnary = IsUnary(right);

            return (isLeftUnary, isRightUnary) switch
            {
                (false, false) => _searcher.And(Evaluate(expression.Left, isNegated, take, scoreFunction), Evaluate(expression.Right, isNegated, take, scoreFunction)),
                (true, true) => EvaluateUnary(right.Operator, EvaluateUnary(left.Operator, _searcher.AllEntries(), left, isNegated, take, scoreFunction), right,
                    isNegated, take, scoreFunction),
                (true, false) => EvaluateUnary(left.Operator, Evaluate(expression.Right, isNegated, take, scoreFunction), left, isNegated, take, scoreFunction),
                (false, true) => EvaluateUnary(right.Operator, Evaluate(expression.Left, isNegated, take, scoreFunction), right, isNegated, take, scoreFunction),
            };
        }

        private IQueryMatch AllEntries<TScoreFunction>(TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            return _searcher.Boost(_searcher.AllEntries(), scoreFunction);
        }

        [SkipLocalsInit]
        private IQueryMatch EvaluateUnary<TScoreFunction>(OperatorType type, in IQueryMatch previousMatch, BinaryExpression value, bool isNegated, int take,
            TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var (fieldName, fieldValue) = GetQueryData(value.Left, value.Right as ValueExpression);
            var fieldId = GetFieldIdInIndex(fieldName);
            
            var operation = UnaryMatchOperationTranslator(type);
            var match = fieldValue switch
            {
                double d => _searcher.UnaryQuery(previousMatch, fieldId, d, operation, take),
                string or LazyStringValue => _searcher.UnaryQuery(previousMatch, fieldId, fieldValue.ToString(), operation, take),
                long l => _searcher.UnaryQuery(previousMatch, fieldId, l, operation, take),
                bool boolean => _searcher.UnaryQuery(previousMatch, fieldId, boolean ? "true" : "false", operation, take),
                _ => throw new EvaluateException($"Got {type} and the value: {fieldValue} at UnaryMatch.")
            };

            return scoreFunction is NullScoreFunction
                ? match
                : _searcher.Boost(match, scoreFunction);
        }

        private IQueryMatch EvaluateBetween<TScoreFunction>(BetweenExpression betweenExpression, bool negated, int take, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var exprMin = betweenExpression.Min.GetValue(_query.QueryParameters);
            var exprMax = betweenExpression.Max.GetValue(_query.QueryParameters);
            var fieldId = GetFieldIdInIndex(GetField(betweenExpression.Source));

            IQueryMatch match = (exprMin, exprMax) switch
            {
                (long min, long max) => _searcher.Between(_searcher.AllEntries(), fieldId, min, max, negated, take),
                (string min, string max) => _searcher.Between(_searcher.AllEntries(), fieldId, min, max, negated, take),
                (double min, double max) => _searcher.Between(_searcher.AllEntries(), fieldId, min, max, negated, take),
                (LazyNumberValue min, LazyNumberValue max) => _searcher.Between(_searcher.AllEntries(), fieldId, (double)min, (double)max, negated, take),
                _ => throw new EvaluateException(
                    $"Got {(exprMin is long or double or string ? exprMax : exprMin)} but expected: {typeof(long)}, {typeof(double)}, {typeof(string)}.")
            };

            return scoreFunction is NullScoreFunction
                ? match
                : _searcher.Boost(match, scoreFunction);
        }

        private IQueryMatch EvaluateInExpression<TScoreFunction>(FieldExpression f, List<QueryExpression> list, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var values = new List<string>();
            foreach (ValueExpression v in list)
            {
                var value = v.GetValue(_query.QueryParameters);
                if (value is BlittableJsonReaderArray bjra)
                    BlittableArrayToListOfString(values, bjra);
                else
                    values.Add(value.ToString());
            }
            var field = GetField(f);
            var fieldId = GetFieldIdInIndex(field);
            QueryData.Add(field, values);
            
            return scoreFunction is NullScoreFunction
                ? _searcher.InQuery(field, values, fieldId)
                : _searcher.InQuery(field, values, scoreFunction, fieldId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static UnaryMatchOperation UnaryMatchOperationTranslator(OperatorType current) => current switch
        {
            OperatorType.Equal => UnaryMatchOperation.Equals,
            OperatorType.NotEqual => UnaryMatchOperation.NotEquals,
            OperatorType.LessThan => UnaryMatchOperation.LessThan,
            OperatorType.GreaterThan => UnaryMatchOperation.GreaterThan,
            OperatorType.LessThanEqual => UnaryMatchOperation.LessThanOrEqual,
            OperatorType.GreaterThanEqual => UnaryMatchOperation.GreaterThanOrEqual,
            _ => throw new ArgumentOutOfRangeException(nameof(current), current, null)
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static OperatorType GetNegated(OperatorType current) =>
            current switch
            {
                OperatorType.LessThan => OperatorType.GreaterThanEqual,
                OperatorType.LessThanEqual => OperatorType.GreaterThan,
                OperatorType.GreaterThan => OperatorType.LessThanEqual,
                OperatorType.GreaterThanEqual => OperatorType.LessThan,
                OperatorType.Equal => OperatorType.NotEqual,
                OperatorType.NotEqual => OperatorType.Equal,
                OperatorType.And => OperatorType.Or,
                OperatorType.Or => OperatorType.And,
                _ => throw new ArgumentOutOfRangeException(nameof(current), current, null)
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static MatchCompareFieldType OrderTypeFieldConverter(OrderByFieldType original) =>
            original switch
            {
                OrderByFieldType.Double => MatchCompareFieldType.Floating,
                OrderByFieldType.Long => MatchCompareFieldType.Integer,
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Integer,
                _ => MatchCompareFieldType.Sequence
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetFieldIdInIndex(string fieldName, bool isFieldType = true)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (_fieldsToFetch is null)
                throw new InvalidQueryException("Field doesn't found in Index.");

            if (isFieldType && _fieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField))
            {
                return indexField.Id;
            }

            return fieldName switch
            {
                Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName or Client.Constants.Documents.Indexing.Fields.DocumentIdMethodName => 0,
                "score" => ScoreId,
                _ => throw new InvalidDataException($"Field {fieldName} does not found in current index.")
            };
        }

        private (string FieldName, object Value) GetQueryData(QueryExpression query, ValueExpression value)
        {
            var fieldValue = GetFieldValueRaw(value);
            var fieldName = GetField(query);

            AddTermToQueryData(fieldName, fieldValue);
            
            return (fieldName, fieldValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddTermToQueryData(string fieldName, object fieldValue)
        {
            if (_query.Metadata.HasHighlightings == false)
                return;

            if (QueryData.TryAdd(fieldName, fieldValue) == false)
            {
                if (QueryData[fieldName] is List<object> listFromDict)
                {
                    listFromDict.Add(fieldValue);
                }
                else
                {
                    QueryData[fieldName] = new List<object>()
                    {
                        QueryData[fieldName],
                        fieldValue
                    };
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private object GetFieldValueRaw(ValueExpression f) => f.GetValue(_query.QueryParameters) switch
        {
            LazyStringValue {Length: 0} or Sparrow.StringSegment {Length: 0} => Client.Constants.Documents.Indexing.Fields.EmptyString,
            null => Client.Constants.Documents.Indexing.Fields.NullValue,
            var value => value
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsUnary(BinaryExpression binaryExpression) => binaryExpression is not null && binaryExpression.IsRangeOperation;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetField(QueryExpression queryExpression) => queryExpression switch
        {
            FieldExpression fieldExpression => _query.Metadata.GetIndexFieldName(fieldExpression, _query.QueryParameters).Value,
            ValueExpression valueExpression => valueExpression.Token.Value,
            MethodExpression methodExpression => methodExpression.Name.Value switch
            {
                Client.Constants.Documents.Indexing.Fields.DocumentIdMethodName => Raven.Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName,
                _ => methodExpression.Name.Value
            },
            _ => throw new InvalidDataException("Unknown type for now.")
        };

        private enum ComparerType
        {
            Ascending,
            Descending,
            Boosting
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ComparerType GetComparerType(bool ascending, int fieldId) => (ascending, fieldId) switch
        {
            (_, ScoreId) => ComparerType.Boosting,
            (true, _) => ComparerType.Ascending,
            (false, _) => ComparerType.Descending
        };

        private IQueryMatch OrderBy(IQueryMatch match, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orders, int take)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            switch (orders.Count)
            {
                //Note: we want to use generics up to 3 comparers. This way we gonna avoid virtual calls in most cases.
                case 1:
                {
                    var fe = orders[0].Expression as FieldExpression;
                    var me = orders[0].Expression as MethodExpression;
                    if (fe is null && me is null)
                        throw new InvalidQueryException($"The expression used in the ORDER BY clause is wrong.");

                    var id = fe is null ? GetFieldIdInIndex(me.Name.Value, false) : GetFieldIdInIndex(GetField(fe));
                    var orderTypeField = OrderTypeFieldConverter(orders[0].FieldType);

                    match = id switch
                    {
                        ScoreId => _searcher.OrderByScore(match, take),
                        >= 0 => orders[0].Ascending
                            ? _searcher.OrderByAscending(match, id, orderTypeField, take)
                            : _searcher.OrderByDescending(match, id, orderTypeField, take),
                        _ => throw new InvalidDataException("Unknown fieldId in ORDER BY clause.")
                    };


                    return match;
                }
                case 2:
                {
                    var firstOrder = orders[0];
                    var secondOrder = orders[1];
                    var firstExpression = firstOrder.Expression as FieldExpression;
                    var secondExpression = secondOrder.Expression as FieldExpression;
                    var firstMethod = firstOrder.Expression as MethodExpression;
                    var secondMethod = secondOrder.Expression as MethodExpression;

                    if (firstExpression is null && firstMethod is null || secondExpression is null && secondMethod is null)
                    {
                        throw new InvalidQueryException($"The expression used in the ORDER BY clause is wrong.");
                    }

                    var firstId = firstExpression is null ? GetFieldIdInIndex(firstMethod.Name.Value, false) : GetFieldIdInIndex(GetField(firstExpression));
                    var firstTypeField = OrderTypeFieldConverter(firstOrder.FieldType);
                    var secondId = secondExpression is null ? GetFieldIdInIndex(secondMethod.Name.Value, false) : GetFieldIdInIndex(GetField(secondExpression));
                    var secondTypeField = OrderTypeFieldConverter(secondOrder.FieldType);

                    return (GetComparerType(firstOrder.Ascending, firstId), GetComparerType(secondOrder.Ascending, secondId)) switch
                    {
                        (ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            default(BoostingComparer)),

                        (ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            default(BoostingComparer)),

                        (ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                            default(BoostingComparer),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                            default(BoostingComparer),
                            default(BoostingComparer)),
                        var (type1, type2) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2})")
                    };
                }
                case 3:
                {
                    var firstOrder = orders[0];
                    var secondOrder = orders[1];
                    var thirdOrder = orders[2];
                    var firstExpression = firstOrder.Expression as FieldExpression;
                    var secondExpression = secondOrder.Expression as FieldExpression;
                    var thirdExpression = thirdOrder.Expression as FieldExpression;
                    var firstMethod = firstOrder.Expression as MethodExpression;
                    var secondMethod = secondOrder.Expression as MethodExpression;
                    var thirdMethod = thirdOrder.Expression as MethodExpression;


                    if (firstExpression is null && firstMethod is null || secondExpression is null && secondMethod is null ||
                        thirdExpression is null && thirdMethod is null)
                    {
                        throw new InvalidQueryException("The expression used in the ORDER BY clause is wrong.");
                    }

                    var firstId = firstExpression is null ? GetFieldIdInIndex(firstMethod.Name.Value, false) : GetFieldIdInIndex(GetField(firstExpression));
                    var firstTypeField = OrderTypeFieldConverter(firstOrder.FieldType);
                    var secondId = secondExpression is null ? GetFieldIdInIndex(secondMethod.Name.Value, false) : GetFieldIdInIndex(GetField(secondExpression));
                    var secondTypeField = OrderTypeFieldConverter(secondOrder.FieldType);
                    var thirdId = thirdExpression is null ? GetFieldIdInIndex(thirdMethod.Name.Value, false) : GetFieldIdInIndex(GetField(thirdExpression));
                    var thirdTypeField = OrderTypeFieldConverter(thirdOrder.FieldType);


                    return (GetComparerType(firstOrder.Ascending, firstId), GetComparerType(secondOrder.Ascending, secondId),
                            GetComparerType(thirdOrder.Ascending, thirdId)) switch
                        {
                            (ComparerType.Ascending, ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Ascending, ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Ascending, ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),

                            (ComparerType.Ascending, ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Ascending, ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Ascending, ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),

                            (ComparerType.Ascending, ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Ascending, ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Ascending, ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),

                            (ComparerType.Descending, ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Descending, ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Descending, ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),

                            (ComparerType.Descending, ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Descending, ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Descending, ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),

                            (ComparerType.Descending, ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Descending, ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Descending, ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),

                            (ComparerType.Boosting, ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Boosting, ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Boosting, ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),

                            (ComparerType.Boosting, ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Boosting, ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Boosting, ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),

                            (ComparerType.Boosting, ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Boosting, ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                            (ComparerType.Boosting, ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                default(BoostingComparer)),

                            var (type1, type2, type3) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2}, {type3})")
                        };
                }
            }

            var comparers = new IMatchComparer[orders.Count];
            for (int i = 0; i < orders.Count; ++i)
            {
                if (orders[i].Expression is not FieldExpression fe)
                {
                    if (orders[i].Expression is MethodExpression me)
                    {
                        comparers[i] = me.Name.Value switch
                        {
                            "id" => orders[i].Ascending
                                ? new SortingMatch.AscendingMatchComparer(_searcher, 0, OrderTypeFieldConverter(orders[i].FieldType))
                                : new SortingMatch.DescendingMatchComparer(_searcher, 0, OrderTypeFieldConverter(orders[i].FieldType)),
                            "score" => default(BoostingComparer),
                            _ => throw new InvalidDataException($"Unknown {nameof(MethodExpression)} as argument of ORDER BY. Have you mean id() or score()?")
                        };
                        continue;
                    }

                    comparers[i] = default(SortingMultiMatch.NullComparer);
                    continue;
                }


                var id = GetFieldIdInIndex(GetField(fe));
                var orderTypeField = OrderTypeFieldConverter(orders[i].FieldType);
                comparers[i] = orders[i].Ascending
                    ? new SortingMatch.AscendingMatchComparer(_searcher, id, orderTypeField)
                    : new SortingMatch.DescendingMatchComparer(_searcher, id, orderTypeField);
            }

            return orders.Count switch
            {
                2 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1]),
                3 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1], comparers[2]),
                4 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1], comparers[2], comparers[3]),
                5 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4]),
                6 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4], comparers[5]),
                7 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4], comparers[5], comparers[6]),
                8 => SortingMultiMatch.Create(_searcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4], comparers[5], comparers[6],
                    comparers[7]),
                _ => throw new InvalidQueryException("Maximum amount of comparers in ORDER BY clause is 8.")
            };
        }

        internal static void BlittableArrayToListOfString(List<string> values, BlittableJsonReaderArray bjra)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            using (var itr = new BlittableJsonReaderArray.BlittableJsonArrayEnumerator(bjra))
            {
                while (itr.MoveNext())
                {
                    switch (itr.Current)
                    {
                        //todo maciej
                        // case BlittableJsonReaderObject item:
                        //     var clone = item.CloneOnTheSameContext();
                        //     builder.WriteEmbeddedBlittableDocument(clone.BasePointer, clone.Size);
                        //     break;

                        case LazyStringValue item:
                            values.Add(item);
                            break;

                        case long item:
                            values.Add(item.ToString(CultureInfo.InvariantCulture));
                            break;

                        case LazyNumberValue item:
                            values.Add(item);
                            break;

                        case LazyCompressedStringValue item:
                            values.Add(item);
                            break;

                        default:
                            throw new InvalidDataException($"Actual value type is {itr.Current.GetType()}. Should be known serialized type and should not happen. ");
                    }
                }
            }
        }


        public void Dispose()
        {
            _allocator?.Dispose();
        }
    }
}
