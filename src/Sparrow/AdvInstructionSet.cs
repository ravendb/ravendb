using System;

#if NET7_0_OR_GREATER
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
#endif

namespace Sparrow
{
    internal static class AdvInstructionSet
    {
        // PERF: Because all those values are static readonly booleans that are defined during the process of loading the type,
        // the JIT will detect them as such and will use the values instead. 
        // https://alexandrnikitin.github.io/blog/jit-optimization-static-readonly-to-const/

        public static readonly bool IsAcceleratedVector128;
        public static readonly bool IsAcceleratedVector256;
        public static readonly bool IsAcceleratedVector512;

        static AdvInstructionSet()
        {
#if NET7_0_OR_GREATER
            IsAcceleratedVector128 = Vector128.IsHardwareAccelerated;
            IsAcceleratedVector256 = Vector256.IsHardwareAccelerated;
#else
            IsAcceleratedVector128 = false;
            IsAcceleratedVector256 = false;
#endif

#if NET8_0_OR_GREATER
            IsAcceleratedVector512 = Vector512.IsHardwareAccelerated;
#else
            IsAcceleratedVector512 = false;
#endif

            if (Environment.GetEnvironmentVariable("RAVENDB_AdvInstructions_Disable_Simd")?.ToLowerInvariant() == "true")
            {
                // We are disabling the whole SIMD support (at all levels) and activating fallback mechanisms.
                // Some algorithms will not have fallback mechanism and therefore use the vector versions anyways.
                // But this allows us to distinguish the case where when we know that not having the support
                // implies we are better off using a different algorithm altogether.
                IsAcceleratedVector128 = false;
                IsAcceleratedVector256 = false;
                IsAcceleratedVector512 = false;
            }

            switch (Environment.GetEnvironmentVariable("RAVENDB_AdvInstructions_Disable_VectorsBiggerThan")?.ToLowerInvariant())
            {
                case "256":
                    IsAcceleratedVector512 = false;
                    goto case "128";
                case "128":
                    IsAcceleratedVector256 = false;
                    break;
                case "0":
                case "all":
                    IsAcceleratedVector128 = false;
                    break;
            }
        }

        public static class X86
        {
            public static readonly bool IsSupportedSse; // SSE 4.2 + POPCNT
            public static readonly bool IsSupportedAvx256; // Avx2 + FMA
            
            // AVX-512 profile flags
            public static readonly bool IsSupportedAvx512; // All Supported AVX-512 extensions
            public static readonly bool IsSupportedAvx512Basic; // F|BW|VL|DQ (Skylake, Zen 3)
            public static readonly bool IsSupportedAvx512Advanced; // Basic + VBMI (Tiger Lake, Zen 4c AVX-512)

            static X86()
            {

#if NET7_0_OR_GREATER
                IsSupportedSse = Sse42.IsSupported & Popcnt.X64.IsSupported; // We will only enable SSE if it support up to SSE 4.2
                IsSupportedAvx256 = IsSupportedSse & Avx2.IsSupported; // We will only enable AVX if it support up to AVX2
#else
                IsSupportedSse = false;
                IsSupportedAvx256 = false;
#endif
                
#if NET9_0_OR_GREATER
                // Basic AVX-512 (F|BW|VL|DQ)
                IsSupportedAvx512Basic = IsSupportedAvx256 & Avx512F.IsSupported & Avx512BW.IsSupported & Avx512CD.VL.IsSupported & Avx512DQ.IsSupported;

                // Advanced AVX-512 (Basic + CD|VBMI)
                IsSupportedAvx512Advanced = IsSupportedAvx512Basic & Avx512Vbmi.IsSupported & Avx512CD.IsSupported;

                // Full AVX-512 support check (all extensions).
                IsSupportedAvx512 = IsSupportedAvx512Advanced & AvxVnni.IsSupported;
                
#elif NET8_0_OR_GREATER
                // Basic AVX-512 (F|BW|VL|DQ)
                IsSupportedAvx512Basic = IsSupportedAvx256 & Avx512F.IsSupported & Avx512BW.IsSupported & Avx512CD.VL.IsSupported & Avx512DQ.IsSupported;
                
                // Advanced AVX-512 (Basic + CD|VBMI)
                IsSupportedAvx512Advanced = IsSupportedAvx512Basic & Avx512Vbmi.IsSupported & Avx512CD.IsSupported;
                
                // Full AVX-512 support check (all extensions). Right now this is equal to Advanced, but it is not guaranteed
                // to stay that way.
                IsSupportedAvx512 = IsSupportedAvx512Advanced;
#else
                IsSupportedAvx512Basic = false;
                IsSupportedAvx512Advanced = false;
                IsSupportedAvx512 = false;
#endif

                if (Environment.GetEnvironmentVariable("RAVENDB_AdvInstructions_Disable_Simd")?.ToLowerInvariant() == "true")
                {
                    // We are disabling the whole SIMD support (at all levels) and activating fallback mechanisms.
                    // Some algorithms will not have fallback mechanism and therefore use the vector versions without acceleration.
                    // When special operations are needed we can switch on and off accordingly.
                    IsSupportedSse = false;
                    IsSupportedAvx256 = false;
                    IsSupportedAvx512 = false;
                    IsSupportedAvx512Basic = false;
                    IsSupportedAvx512Advanced = false;
                }

                // We assume for simplicity in the testing matrix and to simplify our life that whenever AVX512 also is AVX2, etc.
                // This allow us an easier upgrade path for our algorithms without having to rely on a highly complex architecture selector.
                switch (Environment.GetEnvironmentVariable("RAVENDB_AdvInstructions_Disable_Intel_InstSet")?.ToLowerInvariant())
                {
                    case "sse":
                        IsSupportedSse = false;
                        goto case "avx256";
                    case "avx256":
                        IsSupportedAvx256 = false;
                        goto case "avx512-basic";
                    case "avx512": // This is an alias whose objective is to remain backward compatible
                    case "avx512-basic":
                        IsSupportedAvx512Basic = false;
                        IsSupportedAvx512Advanced = false; // Advanced depends on Basic
                        IsSupportedAvx512 = false; // Full depends on Advanced
                        break;
                    case "avx512-advanced":
                        IsSupportedAvx512Advanced = false;
                        IsSupportedAvx512 = false; // Full depends on Advanced
                        break;                    
                    case "avx512-full":
                        IsSupportedAvx512 = false;
                        break;
                }
            }
        }

        public static class Arm
        {
            public static readonly bool IsSupported;
            public static readonly bool IsSupportedArm64;

            static Arm()
            {
#if NET7_0_OR_GREATER
                IsSupported = AdvSimd.IsSupported;
                IsSupportedArm64 = IsSupported & AdvSimd.Arm64.IsSupported;
#else
                IsSupported = false;
                IsSupportedArm64 = false;
#endif

                if (Environment.GetEnvironmentVariable("RAVENDB_AdvInstructions_Disable_Simd")?.ToLowerInvariant() == "true")
                {
                    // We are disabling the whole SIMD support (at all levels) and activating fallback mechanisms.
                    // Some algorithms will not have fallback mechanism and therefore use the vector versions without acceleration.
                    // When special operations are needed we can switch on and off accordingly.
                    IsSupported = false;
                    IsSupportedArm64 = false;
                }

                // We assume for simplicity in the testing matrix and to simplify our life that whenever NEON is used, etc.
                // This allow us an easier upgrade path for our algorithms without having to rely on a highly complex architecture selector.
                switch (Environment.GetEnvironmentVariable("RAVENDB_AdvInstructions_Disable_Arm_InstSet")?.ToLowerInvariant())
                {
                    case "base":
                        IsSupported = false;
                        goto case "arm64";
                    case "arm64":
                        IsSupportedArm64 = false;
                        break;
                }
            }
        }
    }
}
