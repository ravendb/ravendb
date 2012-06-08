using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;
using System;
using System.Globalization;
using System.Reflection;

namespace Raven.Abstractions.Data
{
	public class Facet
	{
		public FacetMode Mode { get; set; }
		public string Name { get; set; }
		public List<string> Ranges { get; set; }

		public Facet()
		{
			Ranges = new List<string>();
		}
	}

    public class Facet<T>
    {
        public Expression<Func<T, object>> Name { get; set; }
        public List<Expression<Func<T, bool>>> Ranges { get; set; }

        public Facet()
        {
            Ranges = new List<Expression<Func<T, bool>>>();
        }

        public static implicit operator Facet(Facet<T> other)
        {
            var ranges = other.Ranges.Select(x => Facet<T>.Parse(x)).ToList();

            return new Facet
            {
                Name = other.ToString(),
                Mode = (other.Ranges == null || other.Ranges.Count == 0) ?
                                FacetMode.Default : FacetMode.Ranges,
                Ranges = ranges
            };
        }

        public static string Parse(Expression<Func<T, bool>> expr)
        {           
            Expression body = expr.Body;
            Console.WriteLine("\n{0}: {1}", body.GetType().Name, body.ToString());

            var param = (ParameterExpression)expr.Parameters[0];
            var operation = (BinaryExpression)expr.Body;

            if (operation.Left is MemberExpression)
            {
                var subExpressionValue = ParseSubExpression(operation);
                var left = (MemberExpression)operation.Left;
                var expression = GetStringRepresentation(left.Member.Name, operation.NodeType, subExpressionValue);                
                return expression;
            }

            if (body is BinaryExpression)            
            {
                var method = body as BinaryExpression;                
                var left = method.Left as BinaryExpression;
                var right = method.Right as BinaryExpression;
                if ((left == null && right == null) || method.NodeType != ExpressionType.AndAlso)
                    throw new InvalidOperationException("Expression doesn't have the correct sub-expression(s) (expected \"&&\")");

                var leftMember = left.Left as MemberExpression;
                var rightMember = right.Left as MemberExpression;
                var validOperators = (left.NodeType == ExpressionType.LessThan && right.NodeType == ExpressionType.GreaterThan) ||
                                     (left.NodeType == ExpressionType.GreaterThan && right.NodeType == ExpressionType.LessThan);
                var validMemberNames = leftMember != null && rightMember != null && 
                                        leftMember.Member.Name == rightMember.Member.Name;
                if (validOperators && validMemberNames)                    
                {                    
                    return GetStringRepresentation(leftMember.Member.Name, right.NodeType, 
                                                    ParseSubExpression(left), ParseSubExpression(right));
                }
            }
            throw new InvalidOperationException("Members in sub-expression(s) are not the correct types (expected \"<\" and \">\")");
        }

        private static object ParseSubExpression(BinaryExpression operation)
        {
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
                    if (field != null && obj != null)
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
                        //This chokes on annonomyous types!?
                        try
                        {
                            //var value = property.GetValue(right.Member, null);
                            var value = property.GetValue(property, null);
                            return value;
                        }
                        catch (TargetException tEx)
                        {
                            //var properties = TypeDescriptor.GetProperties(right.Member.ReflectedType);
                            //foreach (PropertyDescriptor prop in properties)
                            //{
                            //    var testProp = prop.GetValue(right.Member);
                            //}

                            var test = tEx.Message;
                            var newProp = (right.Expression as MemberExpression).Member.ReflectedType.GetProperty("AnonDate");
                            var value = newProp.GetValue(right.Member, null);
                            return value;
                        }
                    }
                }
            }

            //i.e. new DateTime(10, 4, 2001)
            if (operation.Right is NewExpression)
            {
                var right = (NewExpression)operation.Right;
                var invoke = Expression.Lambda(right).Compile();
                var result = invoke.DynamicInvoke();
                return result;
            }

            throw new InvalidOperationException(String.Format("Unable to parse expression: {0} {1} {2}",
                                    operation.Left.GetType().Name, operation.NodeType, operation.Right.GetType().Name));
        }

        private static string GetStringRepresentation<T>(string fieldName, ExpressionType op, T value)
        {
            var valueAsStr = GetStringValue(value);
            var fullFieldName = value.GetType().FullName == "System.String" ? fieldName : fieldName + "_Range";
            if (op == ExpressionType.LessThan)
                return String.Format("{0}:[NULL TO {1}]", fullFieldName, valueAsStr);
            if (op == ExpressionType.GreaterThan)
                return String.Format("{0}:[{1} TO NULL]", fullFieldName, valueAsStr);
            throw new InvalidOperationException("Unable to parse the given operation " + op + ", into a facet range!!! ");
        }

        private static string GetStringRepresentation<T, U>(string fieldName, ExpressionType op, T lValue, U rValue)
        {
            var lValueAsStr = GetStringValue(lValue);
            var rValueAsStr = GetStringValue(rValue);
            var fullFieldName = lValue.GetType().FullName == "System.String" ? fieldName : fieldName + "_Range";
            if (lValueAsStr != null && rValueAsStr != null)
                return String.Format("{0}:[{1} TO {2}]", fullFieldName, lValueAsStr, rValueAsStr);            
            throw new InvalidOperationException("Unable to parse the given operation " + op + ", into a facet range!!! ");
        }

        private static string GetStringValue<T>(T value)
        {
            //Once this code is in RavenDB, the calls to GetValue(..) below will be replaced with calls to NumberUtil.NumberToString(..)
            //see https://github.com/ayende/ravendb/blob/master/Raven.Abstractions/Indexing/NumberUtil.cs#L14  

            var valueAsStr = String.Empty;
            switch (value.GetType().FullName)
            {
                //The nullable stuff here it a bit wierd, but it helps with trying to cast Value types
                case "System.DateTime":
                    valueAsStr = GetValue(value as DateTime?);
                    break;
                case "System.Int32":
                    valueAsStr = GetValue(value as Int32?);
                    break;
                case "System.Int64":
                    valueAsStr = GetValue(value as Int64?);
                    break;
                case "System.Single":
                    valueAsStr = GetValue(value as Single?);
                    break;
                case "System.Double":
                    valueAsStr = GetValue(value as Double?);
                    break;
                case "System.Decimal":
                    valueAsStr = GetValue(value as Decimal?);
                    break;
                case "System.String":
                    valueAsStr = value.ToString();
                    break;
                default:
                    throw new InvalidOperationException("Unable to parse the given type " + value.GetType().Name + ", into a facet range!!! ");
            }
            return valueAsStr;
        }        

        private static string GetValue(DateTime? value)
        {
            return value.Value.ToString("yyyyMMddHHmmss");
        }

        private static string GetValue(Int32? value) //int
        {
            return string.Format("0x{0:X8}", value.Value);
        }

        private static string GetValue(Int64? value) //long
        {
            return string.Format("0x{0:X16}", value.Value);
        }

        private static string GetValue(Single? value) //float
        {
            return "Fx" + value.Value.ToString("G", CultureInfo.InvariantCulture);
        }

        private static string GetValue(Double? value) //double
        {
            return "Dx" + value.Value.ToString("G", CultureInfo.InvariantCulture);
        }

        private static string GetValue(Decimal? value) //decimal
        {
            return "Dx" + value.Value.ToString("G", CultureInfo.InvariantCulture);
        }
    }
}