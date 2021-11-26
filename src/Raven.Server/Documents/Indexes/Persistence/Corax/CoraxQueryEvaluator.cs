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

        [CanBeNull]
        private FieldsToFetch _fieldsToFetch;

        public CoraxQueryEvaluator(IndexSearcher searcher)
        {
            _allocator = new (SharedMultipleUseFlag.None);
            _searcher = searcher;
        }

        public IQueryMatch Search(IndexQueryServerSide query, FieldsToFetch fieldsToFetch)
        {
            _fieldsToFetch = fieldsToFetch;
            _query = query;
            var match = Evaluate(query.Metadata.Query.Where, false);

            if (query.Metadata.OrderBy != null)
                match = OrderBy(match, query.Metadata.Query.OrderBy);
            return match;
        }

        private IQueryMatch Evaluate(QueryExpression condition, bool isNegated)
        {
            switch (condition)
            {
                case NegatedExpression negatedExpression:
                    isNegated  = !isNegated;
                    return Evaluate(negatedExpression.Expression, isNegated);
                case TrueExpression:
                case null:
                    return _searcher.AllEntries();
                case BetweenExpression betweenExpression:
                    return EvaluateBetween(betweenExpression, isNegated);
                case MethodExpression methodExpression:
                    var expressionType = QueryMethod.GetMethodType(methodExpression.Name.Value);
                    var fieldName = GetField((FieldExpression)methodExpression.Arguments[0]);
                    var fieldId = GetFieldIdInIndex(fieldName);
                    switch (expressionType)
                    {
                        case MethodType.StartsWith:
                            return _searcher.StartWithQuery(fieldName,
                                ((ValueExpression)methodExpression.Arguments[1]).Token.Value, fieldId);
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
                    return BinaryEvaluator(be, isNegated);
            }

            throw new EvaluateException($"Evaluation failed in {nameof(CoraxQueryEvaluator)}.");
        }
        
        private IQueryMatch BinaryEvaluator(BinaryExpression expression, bool isNegated)
        {
            if (isNegated == true)
                expression.Operator = GetNegated(expression.Operator);
            
            switch (expression.Operator)
            {
                case OperatorType.Or:
                    return _searcher.Or(Evaluate(expression.Left, isNegated), Evaluate(expression.Right, isNegated));
                case OperatorType.Equal:
                {
                    var value = (ValueExpression)expression.Right;
                    var field = (FieldExpression)expression.Left;
                    var fieldName = GetField(field);
                    return _searcher.TermQuery(fieldName, value.GetValue(_query.QueryParameters).ToString(), GetFieldIdInIndex(fieldName));
                }
            }

            if (expression.IsRangeOperation)
                return EvaluateUnary(expression.Operator, _searcher.AllEntries(), expression, isNegated);

            var left = expression.Left as BinaryExpression;
            var isLeftUnary = IsUnary(left);
            var right = expression.Right as BinaryExpression;
            var isRightUnary = IsUnary(right);
            
            //After those conditions we are in AND clause
            if (isLeftUnary == false && isRightUnary == false)
                return _searcher.And(Evaluate(expression.Left, isNegated), Evaluate(expression.Right, isNegated));

            if (isLeftUnary && isRightUnary)
                return EvaluateUnary(right.Operator, EvaluateUnary(left.Operator, _searcher.AllEntries(), left, isNegated), right, isNegated);

            if (isLeftUnary && isRightUnary is false)
                return EvaluateUnary(left.Operator, Evaluate(expression.Right, isNegated), left, isNegated);
            
            return EvaluateUnary(right.Operator, Evaluate(expression.Left, isNegated), right, isNegated);
        }
        
        private IQueryMatch EvaluateUnary(OperatorType type, in IQueryMatch previousMatch, BinaryExpression value, bool isNegated)
        {
            var fieldId = GetFieldIdInIndex(GetField((FieldExpression)value.Left));
            var field = GetValue((ValueExpression)value.Right);
            var fieldValue = field.FieldValue;
            
            switch (type, field.ValueType)
            {
                case (OperatorType.LessThan, ValueTokenType.Double):
                    return _searcher.LessThan(previousMatch, fieldId, (double)fieldValue);
                case (OperatorType.LessThan, ValueTokenType.Long):
                    return _searcher.LessThan(previousMatch, fieldId, (long)fieldValue);
                case (OperatorType.LessThan, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out var sliceValue);
                    return _searcher.LessThan(previousMatch, fieldId, sliceValue);
                
                case (OperatorType.LessThanEqual, ValueTokenType.Double):
                    return _searcher.LessThanOrEqual(previousMatch, fieldId, (double)fieldValue);
                case (OperatorType.LessThanEqual, ValueTokenType.Long):
                    return _searcher.LessThanOrEqual(previousMatch, fieldId, (long)fieldValue);
                case (OperatorType.LessThanEqual, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out sliceValue);
                    return _searcher.LessThanOrEqual(previousMatch, fieldId, sliceValue);
                
                case (OperatorType.GreaterThan, ValueTokenType.Double):
                    return _searcher.GreaterThan(previousMatch, fieldId, (double)fieldValue);
                case (OperatorType.GreaterThan, ValueTokenType.Long):
                    return _searcher.GreaterThan(previousMatch, fieldId, (long)fieldValue);
                case (OperatorType.GreaterThan, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out sliceValue);
                    return _searcher.GreaterThan(previousMatch, fieldId, sliceValue);
                
                case (OperatorType.GreaterThanEqual, ValueTokenType.Double):
                    return _searcher.GreaterThanOrEqual(previousMatch, fieldId, (double)fieldValue);
                case (OperatorType.GreaterThanEqual, ValueTokenType.Long):
                    return _searcher.GreaterThanOrEqual(previousMatch, fieldId, (long)fieldValue);
                case (OperatorType.GreaterThanEqual, ValueTokenType.String):
                    Slice.From(_allocator, fieldValue.ToString(), out sliceValue);
                    return _searcher.GreaterThanOrEqual(previousMatch, fieldId, sliceValue);
                
                default:
                    throw new Exception($"Got {type} and the value: {field.ValueType} at UnaryMatch.");
            }
        }

        private IQueryMatch EvaluateBetween(BetweenExpression betweenExpression, bool negated)
        {
            var exprMin = GetValue(betweenExpression.Min);
            var exprMax = GetValue(betweenExpression.Max);
            var fieldId = GetFieldIdInIndex(GetField((FieldExpression)betweenExpression.Source));
            
            switch (exprMin.ValueType, exprMax.ValueType, negated)
            {
                case (ValueTokenType.Long, ValueTokenType.Long, false):
                    return _searcher.Between(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue);
                case (ValueTokenType.Long, ValueTokenType.Long, true):
                    return _searcher.NotBetween(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue);
                
                case (ValueTokenType.Double, ValueTokenType.Double, false):
                    return _searcher.Between(_searcher.AllEntries(), fieldId, (double)exprMin.FieldValue, (double)exprMax.FieldValue);
                case (ValueTokenType.Double, ValueTokenType.Double, true):
                    return _searcher.NotBetween(_searcher.AllEntries(), fieldId, (double)exprMin.FieldValue, (double)exprMax.FieldValue);
                
                case (ValueTokenType.String, ValueTokenType.String, false):
                    return _searcher.Between(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue);
                case (ValueTokenType.String, ValueTokenType.String, true):
                    return _searcher.NotBetween(_searcher.AllEntries(), fieldId, (long)exprMin.FieldValue, (long)exprMax.FieldValue);
                
                
                default:
                    var unsupportedType = exprMin.ValueType is ValueTokenType.Double or ValueTokenType.Long or ValueTokenType.String
                        ? exprMax.ValueType
                        : exprMin.ValueType;
                    throw new InvalidDataException($"Got {unsupportedType} but expected: {ValueTokenType.String}, {ValueTokenType.Long}, {ValueTokenType.Double}.");
            }
        }

        private IQueryMatch EvaluateInExpression(FieldExpression f, List<QueryExpression> list)
        {
            var values = new List<string>();
            foreach (ValueExpression v in list)
                values.Add(v.Token.Value);

            return _searcher.InQuery(f.FieldValue, values);
        }

        private IQueryMatch OrderBy(IQueryMatch match, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orders)
        {
            //todo maciej: add support for function in orderby. Also need to implement this as corax method.
            // e.g. _searcher.OrderBy([fields], [as_], [asc_desc]);
            foreach (var order in orders)
            {
                if (order.Expression is not FieldExpression fe) continue;
                var id = GetFieldIdInIndex(GetField(fe));
                var orderTypeField = OrderTypeFieldConverter(order.FieldType);
                
                match = order.Ascending 
                    ? _searcher.OrderByAscending(match, id, orderTypeField)
                    : _searcher.OrderByDescending(match, id, orderTypeField);
            }

            return match;
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
        private string GetField(FieldExpression f)
        {
            return _query.Metadata.GetIndexFieldName(f, _query.QueryParameters).Value;
        }

        public void Dispose()
        {
            _allocator?.Dispose();
        }
    }
}
