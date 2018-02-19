using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Documents.Indexes;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Queries.Facets
{
    public class RangeFacet : FacetBase
    {
        public RangeFacet()
        {
            Ranges = new List<string>();
        }

        /// <summary>
        /// List of facet ranges.
        /// </summary>
        public List<string> Ranges { get; set; }

        internal override Facet AsFacet()
        {
            return null;
        }

        internal override RangeFacet AsRangeFacet()
        {
            return this;
        }
    }

    public class RangeFacet<T> : FacetBase
    {
        public RangeFacet()
        {
            Ranges = new List<Expression<Func<T, bool>>>();
        }

        /// <summary>
        /// List of facet ranges.
        /// </summary>
        public List<Expression<Func<T, bool>>> Ranges { get; set; }

        internal override Facet AsFacet()
        {
            return null;
        }

        internal override RangeFacet AsRangeFacet()
        {
            return this;
        }

        public static implicit operator RangeFacet(RangeFacet<T> other)
        {
            var ranges = other.Ranges.Select(Parse).ToList();

            return new RangeFacet
            {
                Ranges = ranges,
                Options = other.Options,
                Aggregations = other.Aggregations,
                DisplayFieldName = other.DisplayFieldName
            };
        }

        public static string Parse(Expression<Func<T, bool>> expr)
        {
            return Parse(null, (LambdaExpression)expr);
        }
        
        public static string Parse(string prefix, LambdaExpression expr)
        {
            if (expr.Body is MethodCallExpression mce)
            {
                if (mce.Method.Name == "Any" && mce.Arguments.Count == 2)
                {
                    if (mce.Arguments[0] is MemberExpression src &&
                        mce.Arguments[1] is LambdaExpression le)
                    {
                        return Parse(GetFieldName(prefix, src) , le);
                    }
                }
                throw new InvalidOperationException("Don't know how to translate expression to facets: " + expr);
            }
            
            var operation = (BinaryExpression)expr.Body;

            if (operation.Left is MemberExpression me)
            {
                var fieldName = GetFieldName(prefix, me);
                var subExpressionValue = ParseSubExpression(operation);
                var expression = GetStringRepresentation(fieldName, operation.NodeType, subExpressionValue);
                return expression;
            }

            var left = operation.Left as BinaryExpression;
            var right = operation.Right as BinaryExpression;
            if (left == null || right == null || operation.NodeType != ExpressionType.AndAlso)
                throw new InvalidOperationException($"Range can be only specified using: '&&'. Cannot use: '{operation.NodeType}'");

            var leftMember = left.Left as MemberExpression;
            var rightMember = right.Left as MemberExpression;
            if (leftMember == null || rightMember == null)
            {
                throw new InvalidOperationException("Expressions on both sides of '&&' must point to range field. E.g. x => x.Age > 18 && x.Age < 99");
            }

            var leftFieldName = GetFieldName(prefix, leftMember);
            var rightFieldName = GetFieldName(prefix, rightMember);

            if (leftFieldName != rightFieldName)
            {
                throw new InvalidOperationException($"Different range fields were detected: '{leftFieldName}' and '{rightFieldName}'");
            }

            // option #1: expression has form: x > 5 && x < 10
            var hasForm1 = (left.NodeType == ExpressionType.GreaterThan || left.NodeType == ExpressionType.GreaterThanOrEqual)
                           && (right.NodeType == ExpressionType.LessThan || right.NodeType == ExpressionType.LessThanOrEqual);

            if (hasForm1)
            {
                return GetStringRepresentation(leftFieldName, left.NodeType, right.NodeType, ParseSubExpression(left), ParseSubExpression(right));
            }

            // option #2: expression has form x < 10 && x > 5 --> reverse expression to end up with form #1
            var hasForm2 = (left.NodeType == ExpressionType.LessThan || left.NodeType == ExpressionType.LessThanOrEqual)
                           && (right.NodeType == ExpressionType.GreaterThan || right.NodeType == ExpressionType.GreaterThanOrEqual);

            if (hasForm2)
            {
                return GetStringRepresentation(leftFieldName, right.NodeType, left.NodeType, ParseSubExpression(right), ParseSubExpression(left));
            }

            throw new InvalidOperationException("Members in sub-expression(s) are not the correct types (expected '<', '<=', '>' or '>=')");
        }

        private static string GetFieldName(string prefix, MemberExpression left)
        {
            if (Nullable.GetUnderlyingType(left.Member.DeclaringType) != null)
                return GetFieldName(prefix, ((MemberExpression)left.Expression));

            if (left.Expression is MemberExpression parent)
            {
                return GetFieldName(prefix, parent) + "_" + left.Member.Name;
            }

            if (prefix != null)
                return prefix + "_" + left.Member.Name;
            
            return left.Member.Name;
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
                        var type = expression.Type.GetTypeInfo().IsGenericType ? expression.Type.GenericTypeArguments[0] : expression.Type;
                        return Convert.ChangeType(constant.Value, type);
                    case ExpressionType.Convert:
                        return ParseUnaryExpression((UnaryExpression)operand);
                    case ExpressionType.New:
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

        private static string GetStringRepresentation(string fieldName, ExpressionType leftOp, ExpressionType rightOp, object lValue, object rValue)
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
                    return $"{fieldName} between {GetStringValue(lValue)} and {GetStringValue(rValue)}";

                return $"{GetStringRepresentation(fieldName, leftOp, lValue)} and {GetStringRepresentation(fieldName, rightOp, rValue)}";
            }

            throw new InvalidOperationException("Unable to parse the given operation into a facet range!!! ");
        }

        private static string GetStringValue(object value)
        {
            switch (value.GetType().FullName)
            {
                //The nullable stuff here it a bit weird, but it helps with trying to cast Value types
                case "System.DateTime":
                    return $"'{((DateTime)value).GetDefaultRavenFormat()}'";
                case "System.DateTimeOffset":
                    return $"'{((DateTimeOffset)value).UtcDateTime.GetDefaultRavenFormat()}'";
                case "System.Int32":
                    return NumberUtil.NumberToString((int)value);
                case "System.Int64":
                    return NumberUtil.NumberToString((long)value);
                case "System.Single":
                    return NumberUtil.NumberToString((float)value);
                case "System.Double":
                    return NumberUtil.NumberToString((double)value);
                case "System.Decimal":
                    return NumberUtil.NumberToString((double)(decimal)value);
                case "System.String":
                    return $"'{value}'";
                default:
                    throw new InvalidOperationException("Unable to parse the given type " + value.GetType().Name + ", into a facet range!!! ");
            }
        }

        private static string GetStringRepresentation(string fieldName, ExpressionType op, object value)
        {
            var valueAsStr = GetStringValue(value);
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
