using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Tensors
{
    public static class Types
    {
        public static TTo Cast<TFrom, TTo>(TFrom value)
            where TFrom : unmanaged, INumber<TFrom>
            where TTo : unmanaged, INumber<TTo>
        {
            // This is the simple version, it will be completely evicted.
            if (typeof(TFrom) == typeof(TTo))
                return (TTo)(object)value;

            // CreateTruncating converts the numeric value into the target type,
            // possibly discarding fractional parts (similar to an explicit cast).
            return TTo.CreateTruncating(value);
        }
    }
}
