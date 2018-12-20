
using System;
using System.Globalization;
using Microsoft.Extensions.Primitives;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.AST
{
    public abstract class QueryExpression
    {
        public ExpressionType Type;

        public abstract override string ToString();

        public abstract string GetText(IndexQueryServerSide parent);

        public abstract bool Equals(QueryExpression other);
    }

    public static class QueryExpressionExtensions
    {
        private static object Evaluate(QueryExpression q, BlittableJsonReaderObject value)
        {
            switch (q)
            {
                case FieldExpression field:
                    BlittableJsonTraverser.Default.TryRead(value, field.FieldValue, out var result, out var leftPath);
                    return result;
                default:
                    return null;
            }
        }

        public static bool IsMatchedBy(this QueryExpression where, BlittableJsonReaderObject value, BlittableJsonReaderObject queryParameters, bool shouldCaseSensitiveStringCompare = false)
        {
            switch (where)
            {
                case BetweenExpression between:
                    var src = Evaluate(between.Source, value);
                    var hValue = GetFieldValue(between.Max.Token.Value, between.Max.Value, queryParameters);
                    var lValue = GetFieldValue(between.Min.Token.Value, between.Min.Value, queryParameters);

                    if (Compare(lValue, src, shouldCaseSensitiveStringCompare) < 0)
                        return false;

                    if (Compare(src, hValue, shouldCaseSensitiveStringCompare) > 0)
                        return false;

                    return true;
                case TrueExpression _:
                    return true;

                case BinaryExpression be:
                    return IsMatchedBy(be, value, queryParameters, shouldCaseSensitiveStringCompare);
                case NegatedExpression not:
                    var result = IsMatchedBy(not.Expression, value, queryParameters, shouldCaseSensitiveStringCompare);
                    return !result;
                case InExpression ine:
                    var inSrc = Evaluate(ine.Source, value);
                    int matches = 0;
                    foreach (var item in ine.Values)
                    {
                        var val = GetValue(item, value, queryParameters);
                        if (val is BlittableJsonReaderArray array)
                        {
                            foreach (var el in array)
                            {
                                if (AreEqual(inSrc, el, shouldCaseSensitiveStringCompare))
                                {
                                    if (ine.All == false)
                                        return true;
                                    matches++;
                                }
                            }
                        }
                        else
                        {
                            if (AreEqual(inSrc, val, shouldCaseSensitiveStringCompare))
                            {
                                if (ine.All == false)
                                    return true;
                                matches++;
                            }
                        }
                    }
                    return matches == ine.Values.Count;
                case MethodExpression me:
                    var methodType = QueryMethod.GetMethodType(me.Name.Value);
                    switch (methodType)
                    {
                        case MethodType.Exact:
                            if (me.Arguments.Count == 1)
                            {
                                return me.Arguments[0].IsMatchedBy(value, queryParameters, true);
                            }

                            break;
                    }
                    throw new InvalidOperationException("Unsupported edge method invocation in where clause: " + where);
                default:
                    throw new InvalidOperationException("Unsupported edge where expression: " + where);
            }
        }

        private static bool IsMatchedBy(BinaryExpression be, BlittableJsonReaderObject value, BlittableJsonReaderObject queryParameters, bool shouldCaseSensitiveStringCompare)
        {
            switch (be.Operator)
            {
                case OperatorType.And:
                    return IsMatchedBy(be.Left, value, queryParameters) && IsMatchedBy(be.Right, value, queryParameters, shouldCaseSensitiveStringCompare);
                case OperatorType.Or:
                    return IsMatchedBy(be.Left, value, queryParameters) || IsMatchedBy(be.Right, value, queryParameters, shouldCaseSensitiveStringCompare);
            }

            var left = GetValue(be.Left, value, queryParameters);
            var right = GetValue(be.Right, value, queryParameters);

            switch (be.Operator)
            {
                case OperatorType.Equal:
                    return AreEqual(left, right, shouldCaseSensitiveStringCompare);
                case OperatorType.NotEqual:
                    return AreEqual(left, right, shouldCaseSensitiveStringCompare) == false;
                case OperatorType.LessThan:
                    return Compare(left, right, shouldCaseSensitiveStringCompare) < 0;
                case OperatorType.GreaterThan:
                    return Compare(left, right, shouldCaseSensitiveStringCompare) > 0;
                case OperatorType.LessThanEqual:
                    return Compare(left, right, shouldCaseSensitiveStringCompare) <= 0;
                case OperatorType.GreaterThanEqual:
                    return Compare(left, right, shouldCaseSensitiveStringCompare) >= 0;

                default:
                    throw new NotSupportedException("Cannot handle edge query: " + be.Operator + ", " + be);
            }
        }

        private static bool AreEqual(object left, object right, bool shouldCaseSensitiveStringCompare)
        {
            switch (left)
            {
                case BlittableJsonReaderObject leftBlittableJson when right is BlittableJsonReaderObject rightBlittableJson:
                    return leftBlittableJson.Equals(rightBlittableJson, ignoreRavenProperties: true);
                    // Note:
                    // We shouldn't throw here if right is not an object because of polymorphism considerations -
                    // some documents may contain objects and some non-objects with the same property name
                case long l:
                    switch (right)
                    {
                        case double d:
                            return l == (long)d;
                        case long rl:
                            return l == rl;
                        case LazyNumberValue lnv:
                            return l == (long)lnv;
                        default:
                            return false;
                    }
                case LazyNumberValue lnv:
                    return lnv.TryCompareTo(right) == 0;
                case double d:
                    switch (right)
                    {
                        case double rd:
                            return d == rd;
                        case long rl:
                            return d == (double)rl;
                        case LazyNumberValue lnv:
                            return d == (double)lnv;
                        default:
                            return false;
                    }
                case string s:
                    return AreEqual(s, shouldCaseSensitiveStringCompare);
                case StringSegment seg:
                    switch (right)
                    {
                        case string rs:
                            return seg.Equals(rs, shouldCaseSensitiveStringCompare?StringComparison.Ordinal:StringComparison.OrdinalIgnoreCase);
                        case StringSegment rseg:
                            return rseg.Equals(seg, shouldCaseSensitiveStringCompare ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                        case LazyStringValue _:
                        case LazyCompressedStringValue _:
                            return seg.Equals(right.ToString(), shouldCaseSensitiveStringCompare ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                        default:
                            return false;
                    }
                case LazyStringValue lsv:
                    return AreEqual(lsv.ToString(), shouldCaseSensitiveStringCompare);
                case LazyCompressedStringValue lcsv:
                    return AreEqual(lcsv.ToString(), shouldCaseSensitiveStringCompare);
                default:
                    return false;
            }

            bool AreEqual(string s, bool caseSensitive)
            {
                switch (right)
                {
                    case string rs:
                        return string.Equals(s, rs, caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                    case StringSegment rseg:
                        return rseg.Equals(s, caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                    case LazyStringValue _:
                    case LazyCompressedStringValue _:
                        return s.Equals(right.ToString(), caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                    default:
                        return false;
                }
            }
        }


        private static int? Compare(object left, object right, bool shouldCaseSensitiveStringCompare)
        {
            switch (left)
            {
                case LazyNumberValue lnv:
                    return lnv.TryCompareTo(right);
                case long l:
                    switch (right)
                    {
                        case double d:
                            return l.CompareTo((long)d);
                        case long rl:
                            return l.CompareTo(rl);
                        case LazyNumberValue lnv:
                            return l.CompareTo((long)lnv);
                        default:
                            return null;
                    }
                case double d:
                    switch (right)
                    {
                        case double rd:
                            return d.CompareTo(rd);
                        case long rl:
                            return d.CompareTo((double)rl);
                        case LazyNumberValue lnv:
                            return d.CompareTo((double)lnv);
                        default:
                            return null;
                    }
                case string s:
                    return Compare(s, shouldCaseSensitiveStringCompare);
                case StringSegment seg:
                    switch (right)
                    {
                        case string rs:
                            return string.Compare(seg.Value, rs, shouldCaseSensitiveStringCompare ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                        case StringSegment rseg:
                            return string.Compare(rseg.Value, seg.Value, shouldCaseSensitiveStringCompare ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                        case LazyStringValue _:
                        case LazyCompressedStringValue _:
                            return string.Compare(seg.Value, right.ToString(), shouldCaseSensitiveStringCompare ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                        default:
                            return null;
                    }
                case LazyStringValue lsv:
                    return Compare(lsv.ToString(), shouldCaseSensitiveStringCompare);
                case LazyCompressedStringValue lcsv:
                    return Compare(lcsv.ToString(), shouldCaseSensitiveStringCompare);
                default:
                    return null;
            }

            int? Compare(string s, bool caseSensitive)
            {
                switch (right)
                {
                    case string rs:
                        return string.Compare(s, rs, caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                    case StringSegment rseg:
                        return string.Compare(s, rseg.Value, caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                    case LazyStringValue _:
                    case LazyCompressedStringValue _:
                        return string.Compare(s, right.ToString(), caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase);
                    default:
                        return null;
                }
            }
        }

        private static object True = true, False = false; // to avoid constant heap allocs
        private static object GetValue(QueryExpression qe, BlittableJsonReaderObject value, BlittableJsonReaderObject queryParameters)
        {
            switch (qe)
            {
                case ValueExpression ve:
                    switch (ve.Value)
                    {
                        case ValueTokenType.Parameter:
                            queryParameters.TryGetMember(ve.Token, out var r);
                            return r;
                        case ValueTokenType.Long:
                            return QueryBuilder.ParseInt64WithSeparators(ve.Token.Value);
                        case ValueTokenType.Double:
                            return double.Parse(ve.Token.AsSpan(), NumberStyles.AllowThousands | NumberStyles.Float, CultureInfo.InvariantCulture);
                        case ValueTokenType.String:
                            return ve.Token;
                        case ValueTokenType.True:
                            return True;
                        case ValueTokenType.False:
                            return False;
                        case ValueTokenType.Null:
                            return null;
                        default:
                            throw new InvalidOperationException("Unknown ValueExpression value: " + ve.Value);
                    }
                case FieldExpression fe:
                    BlittableJsonTraverser.Default.TryRead(value, fe.FieldValue, out var result, out var leftPath);
                    return result;
                default:
                    throw new NotSupportedException("Cannot get value of " + qe.Type + ", " + qe);
            }
        }



        private static object GetFieldValue(string value, ValueTokenType type, BlittableJsonReaderObject queryParameters)
        {
            switch (type)
            {
                case ValueTokenType.Long:
                    return QueryBuilder.ParseInt64WithSeparators(value);
                case ValueTokenType.Double:
                    return double.Parse(value, CultureInfo.InvariantCulture);
                case ValueTokenType.Parameter:
                    queryParameters.TryGet(value, out object o);
                    return o;
                default:
                    return value;
            }
        }
    }
}
