using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Tensors
{
    public static class Arithmetics
    {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<T> MultiplyAddEstimate<T>(Vector256<T> x, Vector256<T> y, Vector256<T> z)
            where T : unmanaged
        {
#if NET9_0_OR_GREATER
            if (typeof(T) == typeof(double))
            {
                return Vector256.MultiplyAddEstimate(x.AsDouble(), y.AsDouble(), z.AsDouble()).As<double, T>();
            }
            else if (typeof(T) == typeof(float))
            {
                return Vector256.MultiplyAddEstimate(x.AsSingle(), y.AsSingle(), z.AsSingle()).As<float, T>();
            }
#else
            if (Fma.IsSupported)
            {
                if (typeof(T) == typeof(float))
                    return Fma.MultiplyAdd(x.AsSingle(), y.AsSingle(), z.AsSingle()).As<float, T>();
                if (typeof(T) == typeof(double))
                    return Fma.MultiplyAdd(x.AsDouble(), y.AsDouble(), z.AsDouble()).As<double, T>();
            }
#endif
            // This version is less accurate numerically.
            return (x * y) + z;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector512<T> MultiplyAddEstimate<T>(Vector512<T> x, Vector512<T> y, Vector512<T> z)
            where T : unmanaged
        {
#if NET9_0_OR_GREATER
            if (typeof(T) == typeof(double))
            {
                return Vector512.MultiplyAddEstimate(x.AsDouble(), y.AsDouble(), z.AsDouble()).As<double, T>();
            }
            else if (typeof(T) == typeof(float))
            {
                return Vector512.MultiplyAddEstimate(x.AsSingle(), y.AsSingle(), z.AsSingle()).As<float, T>();
            }
#else

            if (Fma.IsSupported)
            {
                // PERF: we do the FMA on the upper and lower lanes separately
                Vector256<T> upperX = x.GetUpper();
                Vector256<T> upperY = y.GetUpper();
                Vector256<T> upperZ = z.GetUpper();

                Vector256<T> lowerX = x.GetLower();
                Vector256<T> lowerY = y.GetLower();
                Vector256<T> lowerZ = z.GetLower();

                if (typeof(T) == typeof(float))
                {
                    var upperS = Fma.MultiplyAdd(upperX.AsSingle(), upperY.AsSingle(), upperZ.AsSingle());
                    var lowerS = Fma.MultiplyAdd(lowerX.AsSingle(), lowerY.AsSingle(), lowerZ.AsSingle());
                    return Vector512.Create(upperS, lowerS).As<float, T>();
                }

                if (typeof(T) == typeof(double))
                {
                    var upperS = Fma.MultiplyAdd(upperX.AsDouble(), upperY.AsDouble(), upperZ.AsDouble());
                    var lowerS = Fma.MultiplyAdd(lowerX.AsDouble(), lowerY.AsDouble(), lowerZ.AsDouble());
                    return Vector512.Create(upperS, lowerS).As<double, T>();
                }
            }
#endif

            // This version is less accurate numerically.
            return (x * y) + z;
        }
    }
}
