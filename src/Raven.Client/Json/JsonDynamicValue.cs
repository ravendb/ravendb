using System;
using System.Dynamic;
using System.Globalization;
using System.Linq.Expressions;

namespace Raven.Client.Json
{
    public sealed class JsonDynamicValue : DynamicObject, IComparable, IEquatable<JsonDynamicValue>
    {
        public JsonDynamicValue(object value)
        {
            Value = value;
        }

        public object Value { get; }

        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            return TryConvertTo(binder.Type, out result);
        }

        public override bool TryBinaryOperation(BinaryOperationBinder binder, object arg, out object result)
        {
            object other = Unwrap(arg);

            switch (binder.Operation)
            {
                case ExpressionType.Equal:
                    result = ValueEquals(Value, other);
                    return true;
                case ExpressionType.NotEqual:
                    result = ValueEquals(Value, other) == false;
                    return true;
                case ExpressionType.GreaterThan:
                    result = Compare(Value, other) > 0;
                    return true;
                case ExpressionType.GreaterThanOrEqual:
                    result = Compare(Value, other) >= 0;
                    return true;
                case ExpressionType.LessThan:
                    result = Compare(Value, other) < 0;
                    return true;
                case ExpressionType.LessThanOrEqual:
                    result = Compare(Value, other) <= 0;
                    return true;
                case ExpressionType.Add when Value is string || other is string:
                    result = Convert.ToString(Value, CultureInfo.InvariantCulture) + Convert.ToString(other, CultureInfo.InvariantCulture);
                    return true;
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                    return TryArithmetic(binder.Operation, Value, other, out result);
                default:
                    return base.TryBinaryOperation(binder, arg, out result);
            }
        }

        public static bool operator ==(JsonDynamicValue left, JsonDynamicValue right) => ValueEquals(left?.Value, right?.Value);

        public static bool operator !=(JsonDynamicValue left, JsonDynamicValue right) => (left == right) == false;

        public static bool operator ==(JsonDynamicValue left, object right) => ValueEquals(left?.Value, Unwrap(right));

        public static bool operator !=(JsonDynamicValue left, object right) => (left == right) == false;

        public static bool operator ==(object left, JsonDynamicValue right) => ValueEquals(Unwrap(left), right?.Value);

        public static bool operator !=(object left, JsonDynamicValue right) => (left == right) == false;

        public int CompareTo(object obj) => Compare(Value, Unwrap(obj));

        public bool Equals(JsonDynamicValue other) => other != null && ValueEquals(Value, other.Value);

        public override bool Equals(object obj) => obj is JsonDynamicValue other && Equals(other);

        public override int GetHashCode() => IsNumber(Value) ? ToDouble(Value).GetHashCode() : Value?.GetHashCode() ?? 0;

        public override string ToString() => Value == null ? string.Empty : Convert.ToString(Value, CultureInfo.InvariantCulture);

        private bool TryConvertTo(Type type, out object result)
        {
            Type targetType = Nullable.GetUnderlyingType(type) ?? type;

            if (Value == null)
            {
                result = null;
                return targetType.IsValueType == false || targetType != type;
            }

            if (type.IsInstanceOfType(Value))
            {
                result = Value;
                return true;
            }

            if (targetType.IsEnum)
            {
                result = Value is string name ? Enum.Parse(targetType, name, ignoreCase: true) : Enum.ToObject(targetType, Value);
                return true;
            }

            if (Value is string text)
            {
                if (targetType == typeof(Guid))
                {
                    result = Guid.Parse(text);
                    return true;
                }

                if (targetType == typeof(DateTime))
                {
                    result = DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                    return true;
                }

                if (targetType == typeof(DateTimeOffset))
                {
                    result = DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                    return true;
                }

                if (targetType == typeof(TimeSpan))
                {
                    result = TimeSpan.Parse(text, CultureInfo.InvariantCulture);
                    return true;
                }
            }

            if (targetType == typeof(string))
            {
                result = ToString();
                return true;
            }

            if (Value is IConvertible && typeof(IConvertible).IsAssignableFrom(targetType))
            {
                result = Convert.ChangeType(Value, targetType, CultureInfo.InvariantCulture);
                return true;
            }

            result = null;
            return false;
        }

        private static object Unwrap(object value) => value is JsonDynamicValue dynamicValue ? dynamicValue.Value : value;

        private static bool IsNumber(object value) =>
            value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;

        private static double ToDouble(object value) => Convert.ToDouble(value, CultureInfo.InvariantCulture);

        private static bool ValueEquals(object left, object right)
        {
            if (left == null || right == null)
                return left == null && right == null;

            if (IsNumber(left) && IsNumber(right))
                return Compare(left, right) == 0;

            if (left is string leftText && right is string rightText)
                return string.Equals(leftText, rightText, StringComparison.Ordinal);

            return left.Equals(right);
        }

        private static int Compare(object left, object right)
        {
            if (left == null || right == null)
                return left == null ? (right == null ? 0 : -1) : 1;

            if (IsNumber(left) && IsNumber(right))
            {
                if (left is double or float || right is double or float)
                    return ToDouble(left).CompareTo(ToDouble(right));

                return Convert.ToDecimal(left, CultureInfo.InvariantCulture).CompareTo(Convert.ToDecimal(right, CultureInfo.InvariantCulture));
            }

            if (left is string leftText && right is string rightText)
                return string.CompareOrdinal(leftText, rightText);

            if (left is IComparable comparable)
                return comparable.CompareTo(right);

            throw new InvalidOperationException($"Cannot compare '{left.GetType()}' with '{right.GetType()}'.");
        }

        private static bool TryArithmetic(ExpressionType operation, object left, object right, out object result)
        {
            if (IsNumber(left) == false || IsNumber(right) == false)
            {
                result = null;
                return false;
            }

            if (left is double or float || right is double or float)
            {
                double l = ToDouble(left), r = ToDouble(right);
                result = operation switch
                {
                    ExpressionType.Add => l + r,
                    ExpressionType.Subtract => l - r,
                    ExpressionType.Multiply => l * r,
                    ExpressionType.Divide => l / r,
                    _ => l % r
                };
                return true;
            }

            if (left is decimal || right is decimal || operation == ExpressionType.Divide)
            {
                decimal l = Convert.ToDecimal(left, CultureInfo.InvariantCulture), r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
                result = operation switch
                {
                    ExpressionType.Add => l + r,
                    ExpressionType.Subtract => l - r,
                    ExpressionType.Multiply => l * r,
                    ExpressionType.Divide => l / r,
                    _ => l % r
                };
                return true;
            }

            long li = Convert.ToInt64(left, CultureInfo.InvariantCulture), ri = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            result = operation switch
            {
                ExpressionType.Add => li + ri,
                ExpressionType.Subtract => li - ri,
                ExpressionType.Multiply => li * ri,
                _ => li % ri
            };
            return true;
        }
    }
}
