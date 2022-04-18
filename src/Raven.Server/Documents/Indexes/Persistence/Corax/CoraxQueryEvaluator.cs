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
        private IndexFieldsMapping _indexFieldsMapping;

        [CanBeNull]
        private FieldsToFetch _fieldsToFetch;

        public CoraxQueryEvaluator(Index index, IndexSearcher searcher)
        {
            _allocator = new(SharedMultipleUseFlag.None);
            _searcher = searcher;
            _index = index;
        }

        public IQueryMatch Search(IndexQueryServerSide query, FieldsToFetch fieldsToFetch = null, IndexFieldsMapping indexFieldsMapping = null, int take = TakeAll)
        {
            _fieldsToFetch = fieldsToFetch;
            _indexFieldsMapping = indexFieldsMapping;
            _query = query;

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
                    string fieldName = string.Empty;
                    int fieldId;
                    switch (expressionType)
                    {
                        case MethodType.StartsWith:
                            fieldName = GetField(methodExpression.Arguments[0]);
                            fieldId = GetFieldIdInIndex(fieldName);
                            var value = GetFieldValue((ValueExpression)methodExpression.Arguments[1]);
                            if (value is Client.Constants.Documents.Indexing.Fields.NullValue)
                                throw new InvalidQueryException("Method startsWith() expects to get an argument of type String while it got Null");

                            return _searcher.StartWithQuery(fieldName, value.ToString(), scoreFunction, isNegated, fieldId);
                        case MethodType.EndsWith:
                            fieldName = GetField(methodExpression.Arguments[0]);
                            fieldId = GetFieldIdInIndex(fieldName);
                            return _searcher.EndsWithQuery(fieldName, GetFieldValue((ValueExpression)methodExpression.Arguments[1]).ToString(), scoreFunction, isNegated,
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
                        (QueryExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list, isNegated, scoreFunction),
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
            string searchTerm;


            if (expression.Arguments.Count is < 2 or > 3)
                throw new InvalidQueryException($"Invalid amount of parameter in {nameof(MethodType.Search)}.");

            if (expression.Arguments[1] is not ValueExpression searchParam)
                throw new InvalidQueryException($"You need to pass value in second argument of {nameof(MethodType.Search)}.");

            //STRUCTURE: <NAME><SEARCH_VALUE><OPERATOR>
            if (searchParam.Value is ValueTokenType.Parameter)
            {
                if (_query.QueryParameters.TryGet(searchParam.Token.Value, out searchTerm) == false)
                    throw new InvalidQueryException($"Cannot find {searchParam.Token.Value} in query.");
            }
            else
            {
                searchTerm = searchParam.Token.Value;
            }

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

            ValueExpression value;
            string fieldName;
            switch (expression.Operator)
            {
                case OperatorType.Or:
                    return _searcher.Or(Evaluate(expression.Left, isNegated, take, scoreFunction), Evaluate(expression.Right, isNegated, take, scoreFunction));
                case OperatorType.Equal:
                {
                    value = (ValueExpression)expression.Right;
                    fieldName = GetField(expression.Left);
                    var match = _searcher.TermQuery(fieldName, GetFieldValue(value).ToString(), GetFieldIdInIndex(fieldName));
                    return scoreFunction is NullScoreFunction
                        ? match
                        : _searcher.Boost(match, scoreFunction);
                }
                case OperatorType.NotEqual:
                    value = (ValueExpression)expression.Right;
                    fieldName = GetField(expression.Left);
                    return isNegated
                        ? _searcher.TermQuery(fieldName, GetFieldValue(value).ToString(), GetFieldIdInIndex(fieldName))
                        : _searcher.UnaryQuery(_searcher.AllEntries(), GetFieldIdInIndex(fieldName), GetFieldValue(value).ToString(), UnaryMatchOperation.NotEquals);
            }

            if (expression.IsRangeOperation)
                return EvaluateUnary(expression.Operator, _searcher.AllEntries(), expression, isNegated, take, scoreFunction);

            var left = expression.Left as BinaryExpression;
            var isLeftUnary = IsUnary(left);
            var right = expression.Right as BinaryExpression;
            var isRightUnary = IsUnary(right);


            return (isLeftUnary, isRightUnary) switch
            {
                (false, false) => DecideWhichAndTreeBuild(expression.Left, expression.Right),
                (true, true) => EvaluateUnary(right.Operator, EvaluateUnary(left.Operator, _searcher.AllEntries(), left, isNegated, take, scoreFunction), right,
                    isNegated, take, scoreFunction),
                (true, false) => EvaluateUnary(left.Operator, Evaluate(expression.Right, isNegated, take, scoreFunction), left, isNegated, take, scoreFunction),
                (false, true) => EvaluateUnary(right.Operator, Evaluate(expression.Left, isNegated, take, scoreFunction), right, isNegated, take, scoreFunction),
            };


            IQueryMatch DecideWhichAndTreeBuild(QueryExpression leftChild, QueryExpression rightChild)
            {
                if (expression.Left is NegatedExpression negatedExpression && expression.Right is not NegatedExpression)
                    return _searcher.AndNot(Evaluate(negatedExpression.Expression, isNegated, take, scoreFunction), Evaluate(expression.Left, isNegated, take, scoreFunction));

                if (expression.Left is not NegatedExpression && expression.Right is NegatedExpression negatedExpression2)
                    return _searcher.AndNot(Evaluate(expression.Left, isNegated, take, scoreFunction), Evaluate(negatedExpression2.Expression, isNegated, take, scoreFunction));

                return _searcher.And(Evaluate(expression.Left, isNegated, take, scoreFunction), Evaluate(expression.Right, isNegated, take, scoreFunction));
            }
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
            var fieldId = GetFieldIdInIndex(GetField((FieldExpression)value.Left));
            var field = GetValue((ValueExpression)value.Right);
            var fieldValue = field.FieldValue;
            var operation = UnaryMatchOperationTranslator(type);
            var match = field.ValueType switch
            {
                ValueTokenType.Double => _searcher.UnaryQuery(previousMatch, fieldId, (double)fieldValue, operation, take),
                ValueTokenType.String => _searcher.UnaryQuery(previousMatch, fieldId, fieldValue.ToString(), operation, take),
                ValueTokenType.Long => _searcher.UnaryQuery(previousMatch, fieldId, (long)fieldValue, operation, take),
                _ => throw new EvaluateException($"Got {type} and the value: {field.ValueType} at UnaryMatch.")
            };

            return scoreFunction is NullScoreFunction
                ? match
                : _searcher.Boost(match, scoreFunction);
        }

        private IQueryMatch EvaluateBetween<TScoreFunction>(BetweenExpression betweenExpression, bool negated, int take, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var exprMin = GetValue(betweenExpression.Min);
            var exprMax = GetValue(betweenExpression.Max);
            var fieldId = GetFieldIdInIndex(GetField(betweenExpression.Source));

            IQueryMatch match = (exprMin.ValueType, exprMax.ValueType) switch
            {
                (ValueTokenType.Long, ValueTokenType.Long) => _searcher.Between(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue,
                    (long)exprMax.FieldValue, negated, take),
                (ValueTokenType.String, ValueTokenType.String) => _searcher.Between(_searcher.AllEntries(), fieldId, (string)exprMin.FieldValue,
                    (string)exprMax.FieldValue, negated, take),
                (ValueTokenType.Double, ValueTokenType.Double) => _searcher.Between(_searcher.AllEntries(), fieldId, (double)exprMin.FieldValue,
                    (double)exprMax.FieldValue, negated, take),
                _ => throw new EvaluateException(
                    $"Got {(exprMin.ValueType is ValueTokenType.Double or ValueTokenType.Long or ValueTokenType.String ? exprMax.ValueType : exprMin.ValueType)} but expected: {ValueTokenType.String}, {ValueTokenType.Long}, {ValueTokenType.Double}.")
            };

            return scoreFunction is NullScoreFunction
                ? match
                : _searcher.Boost(match, scoreFunction);
        }

        private IQueryMatch EvaluateInExpression<TScoreFunction>(QueryExpression f, List<QueryExpression> list, bool isNegated, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var values = new List<string>();
            foreach (ValueExpression v in list)
            {
                var value = GetFieldValue(v);
                if (value is BlittableJsonReaderArray bjra)
                {
                    BlittableArrayToListOfString(values, bjra);
                }
                else
                {
                    values.Add(value.ToString());
                }
            }

            var field = GetField(f);
            var fieldId = GetFieldIdInIndex(field);
            return (scoreFunction, isNegated) switch
            {
                (NullScoreFunction, false) => _searcher.InQuery(field, values, fieldId),
                (_, false) => _searcher.InQuery(field, values, fieldId),
                (NullScoreFunction, true) => _searcher.NotInQuery(field,  _searcher.AllEntries(), values, fieldId),
                (_, true) => _searcher.NotInQuery(field, _searcher.AllEntries(), values, fieldId),
            };
        }

        private (ValueTokenType ValueType, object FieldValue) GetValue(ValueExpression expr)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var valueType = expr.Value;
            object fieldValue = GetFieldValue(expr);
         
            switch (fieldValue)
            {
                case LazyNumberValue fV:
                    valueType = ValueTokenType.Double;
                    fieldValue = fV.ToDouble(CultureInfo.InvariantCulture);
                    break;
                case long:
                    valueType = ValueTokenType.Long;
                    break;
                case decimal value:
                    valueType = ValueTokenType.Double;
                    fieldValue = Convert.ToDouble(value);
                    break;
                case double:
                    valueType = ValueTokenType.Double;
                    break;
                case Sparrow.StringSegment stringSegment:
                    valueType = ValueTokenType.String;
                    fieldValue = stringSegment.ToString();
                break;
                case LazyStringValue lsv:
                    valueType = ValueTokenType.String;
                    fieldValue = lsv.ToString();
                    break;

                default:
                    throw new NotSupportedException($"Unsupported type: {fieldValue}.");
            }

            return (valueType, fieldValue);
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
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Sequence,
                _ => MatchCompareFieldType.Sequence
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetFieldIdInIndex(string fieldName, bool isFieldType = true)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            
            
            if (_fieldsToFetch is null)
            {
                if (_indexFieldsMapping is not null)
                {
                    if (_indexFieldsMapping.TryGetByFieldName(fieldName, out var binding))
                    {
                        return binding.FieldId;
                    }
                }
                
                throw new InvalidQueryException("Field doesn't found in Index.");
            }

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private object GetFieldValue(ValueExpression f) => f.GetValue(_query.QueryParameters) switch
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
            Boosting,
            AscendingAlphanumeric,
            DescendingAlphanumeric
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ComparerType GetComparerType(bool ascending, OrderByFieldType original, int fieldId) => (ascending, original, fieldId) switch
        {
            (true, OrderByFieldType.AlphaNumeric, _) => ComparerType.AscendingAlphanumeric,
            (false, OrderByFieldType.AlphaNumeric, _) => ComparerType.DescendingAlphanumeric,
            (_, _, ScoreId) => ComparerType.Boosting,
            (true, _, _) => ComparerType.Ascending,
            (false, _, _) => ComparerType.Descending,
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
                    var sortingType = orders[0].FieldType == OrderByFieldType.AlphaNumeric ? SortingType.Alphanumerical : SortingType.Normal;
                    match = (id) switch
                    {
                        (ScoreId) => _searcher.OrderByScore(match, take),
                        (>= 0) => orders[0].Ascending
                            ? _searcher.OrderByAscending(match, id, sortingType, orderTypeField, take)
                            : _searcher.OrderByDescending(match, id, sortingType, orderTypeField, take),

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

                    return (GetComparerType(firstOrder.Ascending, firstOrder.FieldType, firstId),
                            GetComparerType(secondOrder.Ascending, secondOrder.FieldType, secondId)) switch
                        {
                            (ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Ascending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Ascending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Descending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Descending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer)),
                            (ComparerType.Boosting, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.Boosting, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField)),
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


                    return (GetComparerType(firstOrder.Ascending, firstOrder.FieldType, firstId), GetComparerType(secondOrder.Ascending, firstOrder.FieldType, secondId),
                            GetComparerType(thirdOrder.Ascending, firstOrder.FieldType, thirdId)) switch
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
                            (ComparerType.Ascending, ComparerType.Ascending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.Ascending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Ascending, ComparerType.Descending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.Descending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Ascending, ComparerType.Boosting, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.Boosting, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.AscendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.AscendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.AscendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Ascending, ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.DescendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.DescendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.DescendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Ascending, ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Ascending, ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Descending, ComparerType.Ascending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.Ascending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Descending, ComparerType.Descending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.Descending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Descending, ComparerType.Boosting, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.Boosting, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.AscendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.AscendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.AscendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Descending, ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.DescendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.DescendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.DescendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Descending, ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Descending, ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Boosting, ComparerType.Ascending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.Ascending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Boosting, ComparerType.Descending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.Descending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
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
                            (ComparerType.Boosting, ComparerType.Boosting, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.Boosting, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.AscendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.AscendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.AscendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Boosting, ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.DescendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.DescendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.DescendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.Boosting, ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.Boosting, ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Ascending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Ascending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Descending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Descending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Boosting, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.Boosting, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Ascending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Ascending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Ascending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Ascending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Ascending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Descending, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Descending, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Descending, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Descending, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Descending, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Boosting, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Boosting, ComparerType.Descending) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Boosting, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Boosting, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.Boosting, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.Ascending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.Descending) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.Boosting) => SortingMultiMatch.Create(_searcher,
                                match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                default(BoostingComparer)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                            (ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric, ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                                _searcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, firstId, firstTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, secondId, secondTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

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
                            _ => throw new InvalidDataException($"Unknown {nameof(MethodExpression)} as argument of ORDER BY. You can order by id() or score()?")
                        };
                        continue;
                    }

                    comparers[i] = default(SortingMultiMatch.NullComparer);
                    continue;
                }


                var id = GetFieldIdInIndex(GetField(fe));
                var orderTypeField = OrderTypeFieldConverter(orders[i].FieldType);
                var sortingType = orders[i].FieldType == OrderByFieldType.AlphaNumeric ? SortingType.Alphanumerical : SortingType.Normal;

                comparers[i] = (orders[i].Ascending, sortingType) switch
                {
                    (true, SortingType.Normal) => new SortingMatch.AscendingMatchComparer(_searcher, id, orderTypeField),
                    (false, SortingType.Normal) => new SortingMatch.DescendingMatchComparer(_searcher, id, orderTypeField),
                    (true, SortingType.Alphanumerical) => new SortingMatch.AlphanumericAscendingMatchComparer(_searcher, id, orderTypeField),
                    (false, SortingType.Alphanumerical) => new SortingMatch.AlphanumericDescendingMatchComparer(_searcher, id, orderTypeField),
                    _ => throw new InvalidDataException($"Unknown {typeof(SortingMatch)}: {sortingType} at {nameof(CoraxQueryEvaluator)}")
                };
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

        private void BlittableArrayToListOfString(List<string> values, BlittableJsonReaderArray bjra)
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
