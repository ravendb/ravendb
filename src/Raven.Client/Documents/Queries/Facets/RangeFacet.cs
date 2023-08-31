using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session.Tokens;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    public sealed class RangeFacet : FacetBase
    {
        private readonly FacetBase _parent;

        internal RangeFacet(FacetBase parent) : this()
        {
            _parent = parent;
        }
        public RangeFacet()
        {
            Ranges = new List<string>();
        }

        /// <summary>
        /// List of facet ranges.
        /// </summary>
        public List<string> Ranges { get; set; }

        internal override FacetToken ToFacetToken(DocumentConventions conventions, Func<object, string> addQueryParameter)
        {
            if (_parent != null)
                return _parent.ToFacetToken(conventions, addQueryParameter);
            return FacetToken.Create(this, addQueryParameter);
        }

        internal static RangeFacet Create(BlittableJsonReaderObject json)
        {
            if (json == null) 
                throw new ArgumentNullException(nameof(json));

            var facet = new RangeFacet();

            if (json.TryGet(nameof(facet.Ranges), out BlittableJsonReaderArray array) && array != null)
            {
                facet.Ranges = new List<string>();
                foreach (var range in array)
                    facet.Ranges.Add(range.ToString());
            }

            Fill(facet, json);

            return facet;
        }
    }

    public sealed class RangeFacet<T> : FacetBase
    {
        public RangeFacet()
        {
            Ranges = new List<Expression<Func<T, bool>>>();
        }

        /// <summary>
        /// List of facet ranges.
        /// </summary>
        public List<Expression<Func<T, bool>>> Ranges { get; set; }

        internal override FacetToken ToFacetToken(DocumentConventions conventions, Func<object, string> addQueryParameter)
        {
            return FacetToken.Create(this, addQueryParameter, conventions);
        }

        public static implicit operator RangeFacet(RangeFacet<T> other)
        {
            var ranges = other.Ranges.Select(r => Parse(r, DocumentConventions.Default)).ToList();

            return new RangeFacet(other)
            {
                Ranges = ranges,
                Aggregations = other.Aggregations,
                DisplayFieldName = other.DisplayFieldName,
            };
        }

        public static string Parse(Expression<Func<T, bool>> expr)
        {
            return Parse(expr, DocumentConventions.Default);
        }
        
        public static string Parse(Expression<Func<T, bool>> expr, DocumentConventions documentConventions)
        {
            return Parse(null, expr, null, documentConventions);
        }

        public static string Parse(string prefix, LambdaExpression expr)
        {
            return Parse(prefix, expr, DocumentConventions.Default);
        }
        
        public static string Parse(string prefix, LambdaExpression expr, DocumentConventions documentConventions)
        {
            return Parse(prefix, expr, null, documentConventions);
        }

        public static string Parse(string prefix, LambdaExpression expr, Func<object, string> addQueryParameter)
        {
            return Parse(prefix, expr, addQueryParameter, DocumentConventions.Default);
        }
        
        public static string Parse(string prefix, LambdaExpression expr, Func<object, string> addQueryParameter, DocumentConventions documentConventions)
        {
            if (expr.Body is MethodCallExpression mce)
            {
                if (mce.Method.Name == "Any" && mce.Arguments.Count == 2)
                {
                    if (mce.Arguments[0] is MemberExpression src &&
                        mce.Arguments[1] is LambdaExpression le)
                    {
                        return Parse(GetFieldName(prefix, src, documentConventions), le, documentConventions);
                    }
                }
                throw new InvalidOperationException("Don't know how to translate expression to facets: " + expr);
            }

            var operation = (BinaryExpression)expr.Body;
            var leftExpression = SkipConvertExpressions(operation.Left);

            if (leftExpression is MemberExpression me)
            {
                var fieldName = GetFieldName(prefix, me, documentConventions);
                var subExpressionValue = ParseSubExpression(operation);
                var expression = GetStringRepresentation(fieldName, operation.NodeType, subExpressionValue, addQueryParameter);
                return expression;
            }

            var rightExpression = SkipConvertExpressions(operation.Right);

            if (!(leftExpression is BinaryExpression left) || 
                !(rightExpression is BinaryExpression right) || 
                operation.NodeType != ExpressionType.AndAlso)
                throw new InvalidOperationException($"Range can be only specified using: '&&'. Cannot use: '{operation.NodeType}'");

            leftExpression = SkipConvertExpressions(left.Left);
            rightExpression = SkipConvertExpressions(right.Left);

            if (!(leftExpression is MemberExpression leftMember) || 
                !(rightExpression is MemberExpression rightMember))
            {
                throw new InvalidOperationException("Expressions on both sides of '&&' must point to range field. E.g. x => x.Age > 18 && x.Age < 99");
            }

            var leftFieldName = GetFieldName(prefix, leftMember, documentConventions);
            var rightFieldName = GetFieldName(prefix, rightMember, documentConventions);

            if (leftFieldName != rightFieldName)
            {
                throw new InvalidOperationException($"Different range fields were detected: '{leftFieldName}' and '{rightFieldName}'");
            }

            // option #1: expression has form: x > 5 && x < 10
            var hasForm1 = (left.NodeType == ExpressionType.GreaterThan || left.NodeType == ExpressionType.GreaterThanOrEqual)
                           && (right.NodeType == ExpressionType.LessThan || right.NodeType == ExpressionType.LessThanOrEqual);

            if (hasForm1)
            {
                return GetStringRepresentation(leftFieldName, left.NodeType, right.NodeType, ParseSubExpression(left), ParseSubExpression(right), addQueryParameter);
            }

            // option #2: expression has form x < 10 && x > 5 --> reverse expression to end up with form #1
            var hasForm2 = (left.NodeType == ExpressionType.LessThan || left.NodeType == ExpressionType.LessThanOrEqual)
                           && (right.NodeType == ExpressionType.GreaterThan || right.NodeType == ExpressionType.GreaterThanOrEqual);

            if (hasForm2)
            {
                return GetStringRepresentation(leftFieldName, right.NodeType, left.NodeType, ParseSubExpression(right), ParseSubExpression(left), addQueryParameter);
            }

            throw new InvalidOperationException("Members in sub-expression(s) are not the correct types (expected '<', '<=', '>' or '>=')");
        }

        private static string GetFieldName(string prefix, MemberExpression left, DocumentConventions conventions)
        {
            if (Nullable.GetUnderlyingType(left.Member.DeclaringType) != null)
                return GetFieldName(prefix, ((MemberExpression)left.Expression), conventions);

            string memberName = conventions.GetConvertedPropertyNameFor(left.Member);
            
            if (left.Expression is MemberExpression parent)
            {
                return GetFieldName(prefix, parent, conventions) + "_" + memberName;
            }

            if (prefix != null)
                return prefix + "_" + memberName;

            return memberName;
        }

        private static object ParseSubExpression(BinaryExpression operation)
        {
            if (operation.Right is UnaryExpression ue)
            {
                return ParseUnaryExpression(ue);
            }

            if (operation.Right is ConstantExpression ce)
            {
                return ce.Value;
            }

            if (operation.Right is MemberExpression me)
            {
                return ParseMemberExpression(me);
            }

            //i.e. new DateTime(10, 4, 2001) || dateTimeVar.AddDays(2) || val +100
            if (operation.Right is NewExpression || operation.Right is MethodCallExpression || operation.Right is BinaryExpression)
            {
                return TryInvokeLambda(operation.Right);
            }

            throw new InvalidOperationException(string.Format("Unable to parse expression: {0} {1} {2}",
                                    operation.Left.GetType().Name, operation.NodeType, operation.Right.GetType().Name));
        }

        private static object ParseMemberExpression(MemberExpression me)
        {
            //http://stackoverflow.com/questions/238765/given-a-type-expressiontype-memberaccess-how-do-i-get-the-field-value
            //http://stackoverflow.com/questions/671968/retrieving-property-name-from-lambda-expression
            if (me.Member is FieldInfo field)
            {
                //This handles x < somefield
                if (me.Expression is ConstantExpression obj)
                {
                    var value = field.GetValue(obj.Value);
                    return value;
                }
                else if (field.IsStatic)
                {
                    var value = field.GetValue(null);
                    return value;
                }
            }
            else
            {
                //This handles things like DateTime.Now
                if (me.Member is PropertyInfo property && me.Member != null)
                {
                    //This chokes on anonymous types!?													
                    var value = property.GetValue(property, null);
                    return value;
                }
            }
            throw new InvalidOperationException(string.Format("Unable to parse expression: {0} {1} {2}",
                             me.GetType().Name, me.NodeType, me.Member));

        }

        private static Expression SkipConvertExpressions(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.ConvertChecked:
                case ExpressionType.Convert:
                    return SkipConvertExpressions(((UnaryExpression)expression).Operand);
                default:
                    return expression;
            }
        }

        private static object ParseUnaryExpression(UnaryExpression expression)
        {
            if (expression.NodeType == ExpressionType.Convert)
            {
                var operand = expression.Operand;

                if (operand is BinaryExpression)
                {
                    return TryInvokeLambda(operand);
                }

                switch (operand.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        return ParseMemberExpression((MemberExpression)operand);
                    case ExpressionType.Constant:
                        var constant = (ConstantExpression)operand;
                        var type = expression.Type.IsGenericType ? expression.Type.GenericTypeArguments[0] : expression.Type;
                        return Convert.ChangeType(constant.Value, type);
                    case ExpressionType.Convert:
                        return ParseUnaryExpression((UnaryExpression)operand);
                    case ExpressionType.New:
                    case ExpressionType.Call:
                        return TryInvokeLambda(operand);
                }
            }

            throw new NotSupportedException("Not supported unary expression type " + expression.NodeType);
        }

        private static object TryInvokeLambda(Expression expression)
        {
            try
            {
                var invoke = Expression.Lambda(expression).Compile();
                return invoke.DynamicInvoke();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not understand expression " + expression, e);
            }
        }

        private static string GetStringRepresentation(string fieldName, ExpressionType leftOp, ExpressionType rightOp, object lValue, object rValue, Func<object, string> addQueryParameter)
        {
            if (lValue is IComparable lValueAsComparable && rValue is IComparable rValueAsComparable)
            {
                if (lValueAsComparable.CompareTo(rValueAsComparable) > 0)
                {
                    throw new InvalidOperationException("Invalid range: " + lValue + ".." + rValue);
                }
            }

            if (lValue != null && rValue != null)
            {
                if (leftOp == ExpressionType.GreaterThanOrEqual && rightOp == ExpressionType.LessThanOrEqual)
                    return $"{fieldName} between {GetStringValue(lValue, addQueryParameter)} and {GetStringValue(rValue, addQueryParameter)}";

                return $"{GetStringRepresentation(fieldName, leftOp, lValue, addQueryParameter)} and {GetStringRepresentation(fieldName, rightOp, rValue, addQueryParameter)}";
            }

            throw new InvalidOperationException("Unable to parse the given operation into a facet range!!! ");
        }

        private static string GetStringValue(object value, Func<object, string> addQueryParameter)
        {
            if (addQueryParameter == null)
            {
                return DefaultGetStringValue(value);
            }
            return "$" + addQueryParameter(value);
        }

        private static string DefaultGetStringValue(object o)
        {
            switch (o)
            {
                case null:
                    return "null";
                case DateTime dt:
                    return "'" + dt.GetDefaultRavenFormat() + "'";
                case DateTimeOffset dto:
                    return "'" + dto.UtcDateTime.GetDefaultRavenFormat(true) + "'";
                case string s:
                    return "'" + EscapeString(s) + "'";
                case int i:
                    return NumberUtil.NumberToString(i);
                case long l:
                    return NumberUtil.NumberToString(l);
                case float f:
                    return NumberUtil.NumberToString(f);
                case double d:
                    return NumberUtil.NumberToString(d);
                case decimal m:
                    return NumberUtil.NumberToString((double)m);
                default:
                    throw new InvalidOperationException("Unable to parse the given type " + o.GetType().Name + ", into a facet range!!! ");
            }
        }

        private static object EscapeString(string s)
        {
            return s.Replace("'", "''");
        }

        private static string GetStringRepresentation(string fieldName, ExpressionType op, object value, Func<object, string> addQueryParameter)
        {
            var valueAsStr = GetStringValue(value, addQueryParameter);
            if (op == ExpressionType.LessThan)
                return $"{fieldName} < {valueAsStr}";
            if (op == ExpressionType.GreaterThan)
                return $"{fieldName} > {valueAsStr}";
            if (op == ExpressionType.LessThanOrEqual)
                return $"{fieldName} <= {valueAsStr}";
            if (op == ExpressionType.GreaterThanOrEqual)
                return $"{fieldName} >= {valueAsStr}";
            throw new InvalidOperationException("Cannot use " + op + " as facet range. Allowed operators: <, <=, >, >=.");
        }
    }
}
