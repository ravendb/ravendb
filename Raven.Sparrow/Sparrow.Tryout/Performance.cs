using BenchmarkDotNet;
using BenchmarkDotNet.Tasks;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Tryout
{
    public static class Performance
    {

        // [BenchmarkTask(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.LegacyJit)]
        [BenchmarkTask(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.RyuJit)]
        public class HashingPerf
        {
            private const int N = 1024 * 4;
            private readonly byte[] data;

            public HashingPerf()
            {
                data = new byte[N];
                new Random(42).NextBytes(data);
            }

            //[Benchmark]
            //public uint XXHash32()
            //{
            //    unsafe
            //    {
            //        int dataLength = data.Length;
            //        fixed (byte* dataPtr = data)
            //        {
            //            return Hashing.XXHash32.CalculateInline(dataPtr, dataLength);
            //        }
            //    }
            //}

            //[Benchmark]
            //public ulong XXHash64()
            //{
            //    unsafe
            //    {
            //        int dataLength = data.Length;
            //        fixed (byte* dataPtr = data)
            //        {
            //            return Hashing.XXHash64.CalculateInline(dataPtr, dataLength);
            //        }
            //    }
            //}

            //[Benchmark]
            //public ulong Metro128()
            //{
            //    unsafe
            //    {
            //        int dataLength = data.Length;
            //        fixed (byte* dataPtr = data)
            //        {
            //            return Hashing.Metro128.CalculateInline(dataPtr, dataLength).H1;
            //        }
            //    }
            //}

            [Benchmark]
            public ulong Metro128Alt()
            {
                unsafe
                {
                    int dataLength = data.Length;
                    fixed (byte* dataPtr = data)
                    {
                        return CalculateInline(dataPtr, dataLength).H1;
                    }
                }
            }

            internal static class Metro128Constants
            {
                public const ulong K0 = 0xC83A91E1;
                public const ulong K1 = 0x8648DBDB;
                public const ulong K2 = 0x7BDEC03B;
                public const ulong K3 = 0x2F5870A5;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static unsafe Metro128Hash CalculateInline(byte* buffer, int length, ulong seed = 0)
            {
                byte* ptr = buffer;
                byte* end = ptr + length;

                ulong v0 = (seed - Metro128Constants.K0) * Metro128Constants.K3;
                ulong v1 = (seed + Metro128Constants.K1) * Metro128Constants.K2;

                if (length >= 32)
                {
                    ulong v2 = (seed + Metro128Constants.K0) * Metro128Constants.K2;
                    ulong v3 = (seed - Metro128Constants.K1) * Metro128Constants.K3;

                    do
                    {
                        v0 += ((ulong*)ptr)[0] * Metro128Constants.K0;
                        v1 += ((ulong*)ptr)[1] * Metro128Constants.K1;

                        v0 = Bits.RotateRight64(v0, 29) + v2;                        
                        v1 = Bits.RotateRight64(v1, 29) + v3;

                        v2 += ((ulong*)ptr)[2] * Metro128Constants.K2;
                        v3 += ((ulong*)ptr)[3] * Metro128Constants.K3;

                        v2 = Bits.RotateRight64(v2, 29) + v0;                        
                        v3 = Bits.RotateRight64(v3, 29) + v1;

                        ptr += 4 * sizeof(ulong);
                    }
                    while (ptr <= (end - 32));

                    v2 ^= Bits.RotateRight64(((v0 + v3) * Metro128Constants.K0) + v1, 21) * Metro128Constants.K1;
                    v3 ^= Bits.RotateRight64(((v1 + v2) * Metro128Constants.K1) + v0, 21) * Metro128Constants.K0;
                    v0 ^= Bits.RotateRight64(((v0 + v2) * Metro128Constants.K0) + v3, 21) * Metro128Constants.K1;
                    v1 ^= Bits.RotateRight64(((v1 + v3) * Metro128Constants.K1) + v2, 21) * Metro128Constants.K0;
                }

                if ((end - ptr) >= 16)
                {
                    v0 += ((ulong*)ptr)[0] * Metro128Constants.K2;
                    v1 += ((ulong*)ptr)[1] * Metro128Constants.K2; 

                    v0 = Bits.RotateRight64(v0, 33) * Metro128Constants.K3;
                    v1 = Bits.RotateRight64(v1, 33) * Metro128Constants.K3;

                    ptr += 2 * sizeof(ulong); 

                    v0 ^= Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 45) * Metro128Constants.K1;
                    v1 ^= Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 45) * Metro128Constants.K0;
                }

                if ((end - ptr) >= 8)
                {
                    v0 += *((ulong*)ptr) * Metro128Constants.K2; ptr += sizeof(ulong); v0 = Bits.RotateRight64(v0, 33) * Metro128Constants.K3;
                    v0 ^= Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 27) * Metro128Constants.K1;
                }

                if ((end - ptr) >= 4)
                {
                    v1 += *((uint*)ptr) * Metro128Constants.K2; ptr += sizeof(uint); v1 = Bits.RotateRight64(v1, 33) * Metro128Constants.K3;
                    v1 ^= Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 46) * Metro128Constants.K0;
                }

                if ((end - ptr) >= 2)
                {
                    v0 += *((ushort*)ptr) * Metro128Constants.K2; ptr += sizeof(ushort); v0 = Bits.RotateRight64(v0, 33) * Metro128Constants.K3;
                    v0 ^= Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 22) * Metro128Constants.K1;
                }

                if ((end - ptr) >= 1)
                {
                    v1 += *((byte*)ptr) * Metro128Constants.K2; v1 = Bits.RotateRight64(v1, 33) * Metro128Constants.K3;
                    v1 ^= Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 58) * Metro128Constants.K0;
                }

                v0 += Bits.RotateRight64((v0 * Metro128Constants.K0) + v1, 13);
                v1 += Bits.RotateRight64((v1 * Metro128Constants.K1) + v0, 37);
                v0 += Bits.RotateRight64((v0 * Metro128Constants.K2) + v1, 13);
                v1 += Bits.RotateRight64((v1 * Metro128Constants.K3) + v0, 37);

                return new Metro128Hash { H1 = v0, H2 = v1 };
            }
        }

        public static void Main(string[] args)
        {
            new BenchmarkCompetitionSwitch(new[] { typeof(HashingPerf) }).Run(args);
        }
    }
}
