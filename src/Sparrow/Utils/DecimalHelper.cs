using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Sparrow.Utils
{
    // http://stackoverflow.com/questions/13477689/find-number-of-decimal-places-in-decimal-value-regardless-of-culture

    public delegate bool IsDoubleDelegate(ref decimal value);

    public class DecimalHelper
    {
        private static readonly string FlagsFieldName;

        public static readonly DecimalHelper Instance;

        static DecimalHelper()
        {
            var decimalType = typeof(decimal);
            if (decimalType.GetField("_flags", BindingFlags.Instance | BindingFlags.NonPublic) != null)
                FlagsFieldName = "_flags";
            else if (decimalType.GetField("flags", BindingFlags.Instance | BindingFlags.NonPublic) != null)
                FlagsFieldName = "flags";
            else
                throw new InvalidOperationException("Could not determine name of the 'flags' field in decimal type");

            Instance = new DecimalHelper();
        }

        public readonly IsDoubleDelegate IsDouble;

        public DecimalHelper()
        {
            IsDouble = CreateIsDoubleMethod().Compile();
        }

        private static Expression<IsDoubleDelegate> CreateIsDoubleMethod()
        {
            var value = Expression.Parameter(typeof(decimal).MakeByRefType(), "value");

            var digits = Expression.RightShift(
                Expression.And(Expression.Field(value, FlagsFieldName), Expression.Constant(~Int32.MinValue, typeof(int))),
                Expression.Constant(16, typeof(int)));

            var hasDecimal = Expression.NotEqual(digits, Expression.Constant(0));

            // return (value.flags & ~Int32.MinValue) >> 16 != 0

            return Expression.Lambda<IsDoubleDelegate>(hasDecimal, value);
        }
    }
}
