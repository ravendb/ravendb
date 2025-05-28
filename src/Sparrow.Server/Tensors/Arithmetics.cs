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
            if (AdvInstructionSet.X86.IsSupportedAvx512Basic)
            {
                if (typeof(T) == typeof(float))
                    return Avx512F.FusedMultiplyAdd(x.AsSingle(), y.AsSingle(), z.AsSingle()).As<float, T>();
                if (typeof(T) == typeof(double))
                    return Avx512F.FusedMultiplyAdd(x.AsDouble(), y.AsDouble(), z.AsDouble()).As<double, T>();
            }

            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                // PERF: we do the FMA on the upper and lower lanes separately
                if (typeof(T) == typeof(float))
                {
                    Vector512<float> fx = x.AsSingle();
                    Vector512<float> fy = y.AsSingle();
                    Vector512<float> fz = z.AsSingle();

                    var upperS = Fma.MultiplyAdd(fx.GetUpper(), fy.GetUpper(), fz.GetUpper());
                    var lowerS = Fma.MultiplyAdd(fx.GetLower(), fy.GetLower(), fz.GetLower());
                    return Vector512.Create(upperS, lowerS).As<float, T>();
                }

                if (typeof(T) == typeof(double))
                {
                    Vector512<double> dx = x.AsDouble();
                    Vector512<double> dy = y.AsDouble();
                    Vector512<double> dz = z.AsDouble();

                    var upperS = Fma.MultiplyAdd(dx.GetUpper(), dy.GetUpper(), dz.GetUpper());
                    var lowerS = Fma.MultiplyAdd(dx.GetLower(), dy.GetLower(), dz.GetLower());
                    return Vector512.Create(upperS, lowerS).As<double, T>();
                }
            }
#endif

            // This version is less accurate numerically.
            return (x * y) + z;
        }
    }
}
