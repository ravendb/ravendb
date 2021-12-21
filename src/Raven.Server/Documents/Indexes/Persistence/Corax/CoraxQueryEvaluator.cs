using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using BinaryExpression = Raven.Server.Documents.Queries.AST.BinaryExpression;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxQueryEvaluator : IDisposable
    {
        private readonly IndexSearcher _searcher;
        private readonly ByteStringContext _allocator;
        private IndexQueryServerSide _query;
        private const int TakeAll = -1;

        [CanBeNull]
        private FieldsToFetch _fieldsToFetch;

        public CoraxQueryEvaluator(IndexSearcher searcher)
        {
            _allocator = new(SharedMultipleUseFlag.None);
            _searcher = searcher;
        }

        public IQueryMatch Search(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, int take = TakeAll)
        {
            _fieldsToFetch = fieldsToFetch;
            _query = query;


            var match = _query.Metadata.Query.Where is null
                ? _searcher.AllEntries()
                : Evaluate(query.Metadata.Query.Where, false, take);

            if (query.Metadata.OrderBy is not null)
                match = OrderBy(match, query.Metadata.Query.OrderBy, take);

            return match;
        }

        private IQueryMatch Evaluate(QueryExpression condition, bool isNegated, int take)
        {
            switch (condition)
            {
                case NegatedExpression negatedExpression:
                    isNegated = !isNegated;
                    return Evaluate(negatedExpression.Expression, isNegated, take);
                case TrueExpression:
                case null:
                    return _searcher.AllEntries();
                case BetweenExpression betweenExpression:
                    return EvaluateBetween(betweenExpression, isNegated, take);
                case MethodExpression methodExpression:
                    var expressionType = QueryMethod.GetMethodType(methodExpression.Name.Value);
                    string fieldName = string.Empty;
                    int fieldId;
                    switch (expressionType)
                    {
                        case MethodType.StartsWith:
                            fieldName = GetField(methodExpression.Arguments[0]);
                            fieldId = GetFieldIdInIndex(fieldName);
                            return _searcher.StartWithQuery(fieldName,
                                ((ValueExpression)methodExpression.Arguments[1]).Token.Value, fieldId);
                        case MethodType.EndsWith:
                            fieldName = GetField(methodExpression.Arguments[0]);
                            fieldId = GetFieldIdInIndex(fieldName);
                            return _searcher.EndsWithQuery(fieldName,
                                ((ValueExpression)methodExpression.Arguments[1]).Token.Value, fieldId);
                        case MethodType.Exact:
                            return BinaryEvaluator((BinaryExpression)methodExpression.Arguments[0], isNegated, take);
                        case MethodType.Search:
                            return SearchMethod(methodExpression);
                        default:
                            throw new NotImplementedException($"Method {nameof(methodExpression)} is not implemented.");
                    }

                case InExpression inExpression:
                    return (inExpression.Source, inExpression.Values) switch
                    {
                        (FieldExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list),
                        _ => throw new NotSupportedException("InExpression.")
                    };

                case BinaryExpression be:
                    return BinaryEvaluator(be, isNegated, take);
            }

            throw new EvaluateException($"Evaluation failed in {nameof(CoraxQueryEvaluator)}.");
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IQueryMatch SearchMethod(MethodExpression expression)
        {
            var fieldName = $"search({GetField(expression.Arguments[0])})";
            var fieldId = GetFieldIdInIndex(fieldName);
            IndexSearcher.SearchOperator @operator = IndexSearcher.SearchOperator.Or;
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
                return _searcher.SearchQuery(fieldName, searchTerm, @operator, fieldId);


            if (expression.Arguments[2] is not FieldExpression operatorType)
                throw new InvalidQueryException($"Expected AND or OR in third argument of {nameof(MethodType.Search)}.");

            @operator = operatorType.FieldValue.ToLowerInvariant() switch
            {
                "and" => IndexSearcher.SearchOperator.And,
                "or" => IndexSearcher.SearchOperator.Or,
                _ => throw new InvalidQueryException($"Expected AND or OR in third argument of {nameof(MethodType.Search)}.")
            };


            return _searcher.SearchQuery(fieldName, searchTerm, @operator, fieldId);
        }

        private IQueryMatch BinaryEvaluator(BinaryExpression expression, bool isNegated, int take)
        {
            if (isNegated == true)
                expression.Operator = GetNegated(expression.Operator);

            switch (expression.Operator)
            {
                case OperatorType.Or:
                    return _searcher.Or(Evaluate(expression.Left, isNegated, take), Evaluate(expression.Right, isNegated, take));
                case OperatorType.Equal:
                {
                    var value = (ValueExpression)expression.Right;
                    var field = (FieldExpression)expression.Left;
                    var fieldName = GetField(field);
                    return _searcher.TermQuery(fieldName, value.GetValue(_query.QueryParameters).ToString(), GetFieldIdInIndex(fieldName));
                }
            }

            if (expression.IsRangeOperation)
                return EvaluateUnary(expression.Operator, _searcher.AllEntries(), expression, isNegated, take);

            var left = expression.Left as BinaryExpression;
            var isLeftUnary = IsUnary(left);
            var right = expression.Right as BinaryExpression;
            var isRightUnary = IsUnary(right);

            //After those conditions we are in AND clause

            return (isLeftUnary, isRightUnary) switch
            {
                (false, false) => _searcher.And(Evaluate(expression.Left, isNegated, take), Evaluate(expression.Right, isNegated, take)),
                (true, true) => EvaluateUnary(right.Operator, EvaluateUnary(left.Operator, _searcher.AllEntries(), left, isNegated, take), right, isNegated, take),
                (true, false) => EvaluateUnary(left.Operator, Evaluate(expression.Right, isNegated, take), left, isNegated, take),
                _ => EvaluateUnary(right.Operator, Evaluate(expression.Left, isNegated, take), right, isNegated, take)
            };
        }

        private IQueryMatch EvaluateUnary(OperatorType type, in IQueryMatch previousMatch, BinaryExpression value, bool isNegated, int take)
        {
            var fieldId = GetFieldIdInIndex(GetField((FieldExpression)value.Left));
            var field = GetValue((ValueExpression)value.Right);
            var fieldValue = field.FieldValue;

            switch (type, field.ValueType)
            {
                case (OperatorType.LessThan, ValueTokenType.Double):
                    return _searcher.LessThan(previousMatch, fieldId, (double)fieldValue, take);
                case (OperatorType.LessThan, ValueTokenType.Long):
                    return _searcher.LessThan(previousMatch, fieldId, (long)fieldValue, take);
                case (OperatorType.LessThan, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out var sliceValue);
                    return _searcher.LessThan(previousMatch, fieldId, sliceValue, take);

                case (OperatorType.LessThanEqual, ValueTokenType.Double):
                    return _searcher.LessThanOrEqual(previousMatch, fieldId, (double)fieldValue, take);
                case (OperatorType.LessThanEqual, ValueTokenType.Long):
                    return _searcher.LessThanOrEqual(previousMatch, fieldId, (long)fieldValue, take);
                case (OperatorType.LessThanEqual, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out sliceValue);
                    return _searcher.LessThanOrEqual(previousMatch, fieldId, sliceValue, take);

                case (OperatorType.GreaterThan, ValueTokenType.Double):
                    return _searcher.GreaterThan(previousMatch, fieldId, (double)fieldValue, take);
                case (OperatorType.GreaterThan, ValueTokenType.Long):
                    return _searcher.GreaterThan(previousMatch, fieldId, (long)fieldValue, take);
                case (OperatorType.GreaterThan, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out sliceValue);
                    return _searcher.GreaterThan(previousMatch, fieldId, sliceValue, take);

                case (OperatorType.GreaterThanEqual, ValueTokenType.Double):
                    return _searcher.GreaterThanOrEqual(previousMatch, fieldId, (double)fieldValue, take);
                case (OperatorType.GreaterThanEqual, ValueTokenType.Long):
                    return _searcher.GreaterThanOrEqual(previousMatch, fieldId, (long)fieldValue, take);
                case (OperatorType.GreaterThanEqual, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out sliceValue);
                    return _searcher.GreaterThanOrEqual(previousMatch, fieldId, sliceValue, take);

                default:
                    throw new EvaluateException($"Got {type} and the value: {field.ValueType} at UnaryMatch.");
            }
        }

        private IQueryMatch EvaluateBetween(BetweenExpression betweenExpression, bool negated, int take)
        {
            var exprMin = GetValue(betweenExpression.Min);
            var exprMax = GetValue(betweenExpression.Max);
            var fieldId = GetFieldIdInIndex(GetField((FieldExpression)betweenExpression.Source));

            switch (exprMin.ValueType, exprMax.ValueType, negated)
            {
                case (ValueTokenType.Long, ValueTokenType.Long, false):
                    return _searcher.Between(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue, take);
                case (ValueTokenType.Long, ValueTokenType.Long, true):
                    return _searcher.NotBetween(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue, take);

                case (ValueTokenType.Double, ValueTokenType.Double, false):
                    return _searcher.Between(_searcher.AllEntries(), fieldId, (double)exprMin.FieldValue, (double)exprMax.FieldValue, take);
                case (ValueTokenType.Double, ValueTokenType.Double, true):
                    return _searcher.NotBetween(_searcher.AllEntries(), fieldId, (double)exprMin.FieldValue, (double)exprMax.FieldValue, take);

                case (ValueTokenType.String, ValueTokenType.String, false):
                    return _searcher.Between(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue, take);
                case (ValueTokenType.String, ValueTokenType.String, true):
                    return _searcher.NotBetween(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue, take);


                default:
                    var unsupportedType = exprMin.ValueType is ValueTokenType.Double or ValueTokenType.Long or ValueTokenType.String
                        ? exprMax.ValueType
                        : exprMin.ValueType;
                    throw new EvaluateException($"Got {unsupportedType} but expected: {ValueTokenType.String}, {ValueTokenType.Long}, {ValueTokenType.Double}.");
            }
        }

        private IQueryMatch EvaluateInExpression(FieldExpression f, List<QueryExpression> list)
        {
            var values = new List<string>();
            foreach (ValueExpression v in list)
                values.Add(v.Token.Value);

            return _searcher.InQuery(f.FieldValue, values);
        }

        private (ValueTokenType ValueType, object FieldValue) GetValue(ValueExpression expr)
        {
            var valueType = expr.Value;
            object fieldValue = GetFieldValue(expr);
            if (valueType != ValueTokenType.Parameter)
                return (valueType, fieldValue);


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
                default:
                    throw new NotSupportedException($"Unsupported type: ${fieldValue}.");
            }

            return (valueType, fieldValue);
        }

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

        private static MatchCompareFieldType OrderTypeFieldConverter(OrderByFieldType original) =>
            original switch
            {
                OrderByFieldType.Double => MatchCompareFieldType.Floating,
                OrderByFieldType.Long => MatchCompareFieldType.Integer,
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Integer,
                _ => MatchCompareFieldType.Sequence
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetFieldIdInIndex(string fieldName)
        {
            if (_fieldsToFetch is null)
                throw new InvalidQueryException("Field doesn't found in Index.");

            if (_fieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField))
            {
                return indexField.Id;
            }

            throw new InvalidDataException($"Field {fieldName} does not found in current index.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private object GetFieldValue(ValueExpression f) => f.GetValue(_query.QueryParameters);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsUnary(BinaryExpression binaryExpression) => binaryExpression is not null && binaryExpression.IsRangeOperation;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetField(QueryExpression queryExpression) => queryExpression switch
        {
            FieldExpression fieldExpression => _query.Metadata.GetIndexFieldName(fieldExpression, _query.QueryParameters).Value,
            ValueExpression valueExpression => valueExpression.Token.Value,
            _ => throw new InvalidDataException("Unknown type for now.")
        };

        private IQueryMatch OrderBy(IQueryMatch match, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orders, int take)
        {
            switch (orders.Count)
            {
                //Note: we want to use generics up to 3 comparers. This way we gonna avoid virtual calls in most cases.
                case 1:
                {
                    if (orders[0].Expression is not FieldExpression fe)
                        throw new InvalidQueryException($"The expression used in the ORDER BY clause is wrong.");
                    var id = GetFieldIdInIndex(GetField(fe));
                    var orderTypeField = OrderTypeFieldConverter(orders[0].FieldType);

                    match = orders[0].Ascending
                        ? _searcher.OrderByAscending(match, id, orderTypeField, take)
                        : _searcher.OrderByDescending(match, id, orderTypeField, take);


                    return match;
                }
                case 2:
                {
                    var firstOrder = orders[0];
                    var secondOrder = orders[1];
                    if (firstOrder.Expression is not FieldExpression first || secondOrder.Expression is not FieldExpression second)
                    {
                        throw new InvalidQueryException($"The expression used in the ORDER BY clause is wrong.");
                    }

                    var firstId = GetFieldIdInIndex(GetField(first));
                    var firstTypeField = OrderTypeFieldConverter(firstOrder.FieldType);
                    var secondId = GetFieldIdInIndex(GetField(second));
                    var secondTypeField = OrderTypeFieldConverter(secondOrder.FieldType);

                    return (firstOrder.Ascending, secondOrder.Ascending) switch
                    {
                        (true, true) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (false, true) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (true, false) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField)),

                        (false, false) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField))
                    };
                }
                case 3:
                {
                    var firstOrder = orders[0];
                    var secondOrder = orders[1];
                    var thirdOrder = orders[2];

                    if (firstOrder.Expression is not FieldExpression first || secondOrder.Expression is not FieldExpression second ||
                        thirdOrder.Expression is not FieldExpression third)
                    {
                        throw new InvalidQueryException("The expression used in the ORDER BY clause is wrong.");
                    }

                    var firstId = GetFieldIdInIndex(GetField(first));
                    var firstTypeField = OrderTypeFieldConverter(firstOrder.FieldType);
                    var secondId = GetFieldIdInIndex(GetField(second));
                    var secondTypeField = OrderTypeFieldConverter(secondOrder.FieldType);
                    var thirdId = GetFieldIdInIndex(GetField(third));
                    var thirdTypeField = OrderTypeFieldConverter(thirdOrder.FieldType);


                    return (firstOrder.Ascending, secondOrder.Ascending, thirdOrder.Ascending) switch
                    {
                        (true, true, true) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (true, true, false) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (true, false, true) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (false, true, true) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (false, false, true) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (false, true, false) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.AscendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (true, false, false) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.AscendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),

                        (false, false, false) => SortingMultiMatch.Create(_searcher, match,
                            new SortingMatch.DescendingMatchComparer(_searcher, firstId, firstTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, secondId, secondTypeField),
                            new SortingMatch.DescendingMatchComparer(_searcher, thirdId, thirdTypeField)),
                    };
                }
            }

            var comparers = new IMatchComparer[orders.Count];
            for (int i = 0; i < orders.Count; ++i)
            {
                if (orders[i].Expression is not FieldExpression fe)
                {
                    comparers[i] = default(SortingMultiMatch.NullComparer);
                    break;
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

        public void Dispose()
        {
            _allocator?.Dispose();
        }
    }
}
