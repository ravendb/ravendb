using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.NewClient.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Abstractions.Util;
using Newtonsoft.Json;
using Raven.NewClient.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Data
{
    public class Facet
    {
        [JsonIgnore]
        private string _displayName;

        /// <summary>
        /// Mode of a facet (Default, Ranges).
        /// </summary>
        public FacetMode Mode { get; set; }

        /// <summary>
        /// Flags indicating type of facet aggregation.
        /// </summary>
        public FacetAggregation Aggregation { get; set; }

        /// <summary>
        /// Field on which aggregation will be performed.
        /// </summary>
        public string AggregationField { get; set; }

        /// <summary>
        /// Type of field on which aggregation will be performed.
        /// </summary>
        public string AggregationType { get; set; }

        /// <summary>
        /// Name of facet.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Display name of facet. Will return {Name} if null.
        /// </summary>
        public string DisplayName
        {
            get { return _displayName ?? Name; }
            set { _displayName = value; }
        }

        /// <summary>
        /// List of facet ranges.
        /// </summary>
        public List<string> Ranges { get; set; }

        /// <summary>
        /// Maximum number of results to return.
        /// </summary>
        public int? MaxResults { get; set; }

        /// <summary>
        /// Indicates how terms should be sorted.
        /// </summary>
        /// <value>FacetTermSortMode.ValueAsc by default.</value>
        public FacetTermSortMode TermSortMode { get; set; }

        /// <summary>
        /// Indicates if remaining terms should be included in results.
        /// </summary>
        public bool IncludeRemainingTerms { get; set; }

        public Facet()
        {
            Ranges = new List<string>();
            TermSortMode = FacetTermSortMode.ValueAsc;
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Mode)] = Mode,
                [nameof(Aggregation)] = Aggregation,
                [nameof(AggregationField)] = AggregationField,
                [nameof(AggregationType)] = AggregationType,
                [nameof(Name)] = Name,
                [nameof(MaxResults)] = MaxResults,
                [nameof(TermSortMode)] = TermSortMode,
                [nameof(IncludeRemainingTerms)] = IncludeRemainingTerms
            };

            if (string.IsNullOrWhiteSpace(_displayName) == false && string.Equals(Name, _displayName) == false)
                json[nameof(DisplayName)] = DisplayName;

            if (Ranges != null)
            {
                var list = new DynamicJsonArray();
                foreach (var range in Ranges)
                    list.Add(range);

                json[nameof(Ranges)] = list;
            }

            return json;
        }
    }

    public class Facet<T>
    {
        /// <summary>
        /// Name of facet.
        /// </summary>
        public Expression<Func<T, object>> Name { get; set; }

        /// <summary>
        /// List of facet ranges.
        /// </summary>
        public List<Expression<Func<T, bool>>> Ranges { get; set; }

        public Facet()
        {
            Ranges = new List<Expression<Func<T, bool>>>();
        }

        public static implicit operator Facet(Facet<T> other)
        {
            if (other.Name == null)
                throw new InvalidOperationException();

            var ranges = other.Ranges.Select(Parse).ToList();

            var shouldUseRanges = other.Ranges != null &&
                                other.Ranges.Count > 0 &&
                                other.Name.Body.Type != typeof(string);
            var mode = shouldUseRanges ? FacetMode.Ranges : FacetMode.Default;

            var name = string.Empty;
            if (other.Name.Body is MemberExpression)
            {
                name = (other.Name.Body as MemberExpression).Member.Name;
            }
            else if (other.Name.Body is UnaryExpression)
            {
                var operand = (other.Name.Body as UnaryExpression).Operand;
                if (operand is MemberExpression)
                {
                    name = (operand as MemberExpression).Member.Name;
                }
            }

            if (mode == FacetMode.Ranges)
            {
                var type = GetExpressionType(other.Name);
                if (type == typeof(int) ||
                    type == typeof(long) ||
                    type == typeof(double) ||
                    type == typeof(short) ||
                    type == typeof(float) ||
                    type == typeof(decimal))
                    name += Constants.Indexing.Fields.RangeFieldSuffix;
            }

            return new Facet
            {
                Name = name,
                Mode = mode,
                Ranges = ranges
            };
        }

        private static Type GetExpressionType(Expression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.Lambda:
                    return GetExpressionType(((LambdaExpression)expr).Body);
                case ExpressionType.Convert:
                    return GetExpressionType(((UnaryExpression)expr).Operand);
                default:
                    return expr.Type;
            }
        }

        public static string Parse(Expression<Func<T, bool>> expr)
        {
            var operation = (BinaryExpression)expr.Body;

            if (operation.Left is MemberExpression)
            {
                var subExpressionValue = ParseSubExpression(operation);
                var expression = GetStringRepresentation(operation.NodeType, subExpressionValue);
                return expression;
            }

            var left = operation.Left as BinaryExpression;
            var right = operation.Right as BinaryExpression;
            if ((left == null || right == null) || operation.NodeType != ExpressionType.AndAlso)
                throw new InvalidOperationException("Range can be only specified using: \"&&\". Cannot use: \"" + operation.NodeType + "\"");

            var leftMember = left.Left as MemberExpression;
            var rightMember = right.Left as MemberExpression;
            if (leftMember == null || rightMember == null)
            {
                throw new InvalidOperationException("Expressions on both sides of \"&&\" must point to range field. Ex. x => x.Age > 18 && x.Age < 99");
            }

            if (GetFieldName(leftMember) != GetFieldName(rightMember))
            {
                throw new InvalidOperationException("Different range fields were detected: \"" + GetFieldName(leftMember) + "\" and \"" + GetFieldName(rightMember) + "\"");
            }

            // option #1: expression has form: x > 5 && x < 10
            var hasForm1 = (left.NodeType == ExpressionType.GreaterThan || left.NodeType == ExpressionType.GreaterThanOrEqual)
                           && (right.NodeType == ExpressionType.LessThan || right.NodeType == ExpressionType.LessThanOrEqual);

            if (hasForm1)
            {
                return GetStringRepresentation(left.NodeType, right.NodeType, ParseSubExpression(left), ParseSubExpression(right));
            }

            // option #2: expression has form x < 10 && x > 5 --> reverse expression to end up with form #1
            var hasForm2 = (left.NodeType == ExpressionType.LessThan || left.NodeType == ExpressionType.LessThanOrEqual)
                           && (right.NodeType == ExpressionType.GreaterThan || right.NodeType == ExpressionType.GreaterThanOrEqual);

            if (hasForm2)
            {
                return GetStringRepresentation(right.NodeType, left.NodeType, ParseSubExpression(right), ParseSubExpression(left));
            }

            throw new InvalidOperationException("Members in sub-expression(s) are not the correct types (expected \"<\", \"<=\", \">\" or \">=\")");
        }

        private static string GetFieldName(MemberExpression left)
        {
            if (Nullable.GetUnderlyingType(left.Member.DeclaringType) != null)
                return GetFieldName(((MemberExpression)left.Expression));
            return left.Member.Name;
        }

        private static object ParseSubExpression(BinaryExpression operation)
        {
            if (operation.Right is UnaryExpression)
            {
                return ParseUnaryExpression((UnaryExpression)operation.Right);
            }

            if (operation.Right is ConstantExpression)
            {
                var right = (ConstantExpression)operation.Right;
                return right.Value;
            }

            //http://stackoverflow.com/questions/238765/given-a-type-expressiontype-memberaccess-how-do-i-get-the-field-value
            //http://stackoverflow.com/questions/671968/retrieving-property-name-from-lambda-expression
            if (operation.Right is MemberExpression)
            {
                var right = (MemberExpression)operation.Right;
                var field = right.Member as FieldInfo;
                if (field != null)
                {
                    //This handles x < somefield
                    var obj = right.Expression as ConstantExpression;
                    if (obj != null)
                    {
                        var value = field.GetValue(obj.Value);
                        return value;
                    }
                }
                else
                {
                    //This handles things like DateTime.Now
                    var property = right.Member as PropertyInfo;
                    if (property != null && right.Member != null)
                    {
                        //This chokes on anonymous types!?													
                        var value = property.GetValue(property, null);
                        return value;
                    }
                }
            }

            //i.e. new DateTime(10, 4, 2001) || dateTimeVar.AddDays(2) || val +100
            if (operation.Right is NewExpression || operation.Right is MethodCallExpression || operation.Right is BinaryExpression)
            {
                try
                {
                    var invoke = Expression.Lambda(operation.Right).Compile();
                    return invoke.DynamicInvoke();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not understand expression " + operation.Right, e);
                }
            }

            throw new InvalidOperationException(string.Format("Unable to parse expression: {0} {1} {2}",
                                    operation.Left.GetType().Name, operation.NodeType, operation.Right.GetType().Name));
        }

        private static object ParseUnaryExpression(UnaryExpression expression)
        {
            if (expression.NodeType == ExpressionType.Convert)
            {
                var operand = expression.Operand;

                switch (operand.NodeType)
                {
                    case ExpressionType.Constant:
                        var constant = (ConstantExpression)operand;
                        var type = expression.Type.GetTypeInfo().IsGenericType ? expression.Type.GenericTypeArguments[0] : expression.Type;
                        return Convert.ChangeType(constant.Value, type);
                    case ExpressionType.Convert:
                        return ParseUnaryExpression((UnaryExpression)operand);
                }
            }

            throw new NotSupportedException("Not supported unary expression type " + expression.NodeType);
        }

        private static string GetStringRepresentation(ExpressionType op, object value)
        {
            var valueAsStr = GetStringValue(value);
            if (op == ExpressionType.LessThan)
                return string.Format("[NULL TO {0}]", valueAsStr);
            if (op == ExpressionType.GreaterThan)
                return string.Format("[{0} TO NULL]", valueAsStr);
            if (op == ExpressionType.LessThanOrEqual)
                return string.Format("[NULL TO {0}}}", valueAsStr);
            if (op == ExpressionType.GreaterThanOrEqual)
                return string.Format("{{{0} TO NULL]", valueAsStr);
            throw new InvalidOperationException("Cannot use " + op + " as facet range. Allowed operators: <, <=, >, >=.");
        }

        private static string GetStringRepresentation(ExpressionType leftOp, ExpressionType rightOp, object lValue, object rValue)
        {
            var lValueAsComparable = lValue as IComparable;
            var rValueAsComparable = rValue as IComparable;

            if (lValueAsComparable != null && rValueAsComparable != null)
            {
                if (lValueAsComparable.CompareTo(rValueAsComparable) > 0)
                {
                    throw new InvalidOperationException("Invalid range: " + lValue + ".." + rValue);
                }
            }
            var lValueAsStr = GetStringValue(lValue);
            var rValueAsStr = GetStringValue(rValue);
            if (lValueAsStr != null && rValueAsStr != null)
                return string.Format("{0}{1} TO {2}{3}", CalculateBraces(leftOp, true), lValueAsStr, rValueAsStr, CalculateBraces(rightOp, false));
            throw new InvalidOperationException("Unable to parse the given operation into a facet range!!! ");
        }

        private static string CalculateBraces(ExpressionType op, bool isLeft)
        {
            if (op == ExpressionType.GreaterThanOrEqual || op == ExpressionType.LessThanOrEqual)
                return isLeft ? "{" : "}";

            return isLeft ? "[" : "]";
        }


        private static string GetStringValue(object value)
        {
            switch (value.GetType().FullName)
            {
                //The nullable stuff here it a bit weird, but it helps with trying to cast Value types
                case "System.DateTime":
                    return RavenQuery.Escape(((DateTime)value).GetDefaultRavenFormat());
                case "System.Int32":
                    return NumberUtil.NumberToString(((int)value));
                case "System.Int64":
                    return NumberUtil.NumberToString((long)value);
                case "System.Single":
                    return NumberUtil.NumberToString((float)value);
                case "System.Double":
                    return NumberUtil.NumberToString((double)value);
                case "System.Decimal":
                    return NumberUtil.NumberToString((double)(decimal)value);
                case "System.String":
                    return RavenQuery.Escape(value.ToString());
                default:
                    throw new InvalidOperationException("Unable to parse the given type " + value.GetType().Name + ", into a facet range!!! ");
            }
        }
    }
}