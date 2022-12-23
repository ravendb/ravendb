
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Sparrow.Server.Utils.VxSort
{
    partial class Sort
    {
        [SkipLocalsInit]
        public unsafe static void Run<T>(T* left, T* right) where T : unmanaged
        { 
            if (!Avx2.IsSupported)
            {
                MemoryExtensions.Sort(new Span<T>(left, (int)(right - left) + 1));
                return;
            }


            if (typeof(T) == typeof(int))
            {
                int* il = (int*)left;
                int* ir = (int*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert(Avx2VectorizedSort.Int32Config.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.Int32Config.Unroll <= 12);
                
                if (length < Avx2VectorizedSort.Int32Config.SmallSortThresholdElements)
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, int.MinValue, int.MaxValue, Avx2VectorizedSort.Int32Config.REALIGN_BOTH, depthLimit);
                return;
            }            

            if (typeof(T) == typeof(uint))
            {
                uint* il = (uint*)left;
                uint* ir = (uint*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert(Avx2VectorizedSort.UInt32Config.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.UInt32Config.Unroll <= 12);
                
                if (length < Avx2VectorizedSort.UInt32Config.SmallSortThresholdElements)
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.UInt32Config.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, uint.MinValue, uint.MaxValue, Avx2VectorizedSort.UInt32Config.REALIGN_BOTH, depthLimit);
                return;
            }            

            if (typeof(T) == typeof(float))
            {
                float* il = (float*)left;
                float* ir = (float*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert(Avx2VectorizedSort.FloatConfig.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.FloatConfig.Unroll <= 12);
                
                if (length < Avx2VectorizedSort.FloatConfig.SmallSortThresholdElements)
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.FloatConfig.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, float.MinValue, float.MaxValue, Avx2VectorizedSort.FloatConfig.REALIGN_BOTH, depthLimit);
                return;
            }            

            if (typeof(T) == typeof(long))
            {
                long* il = (long*)left;
                long* ir = (long*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert(Avx2VectorizedSort.Int64Config.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.Int64Config.Unroll <= 12);
                
                if (length < Avx2VectorizedSort.Int64Config.SmallSortThresholdElements)
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.Int64Config.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, long.MinValue, long.MaxValue, Avx2VectorizedSort.Int64Config.REALIGN_BOTH, depthLimit);
                return;
            }            

            if (typeof(T) == typeof(ulong))
            {
                ulong* il = (ulong*)left;
                ulong* ir = (ulong*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert(Avx2VectorizedSort.UInt64Config.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.UInt64Config.Unroll <= 12);
                
                if (length < Avx2VectorizedSort.UInt64Config.SmallSortThresholdElements)
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.UInt64Config.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, ulong.MinValue, ulong.MaxValue, Avx2VectorizedSort.UInt64Config.REALIGN_BOTH, depthLimit);
                return;
            }            

            if (typeof(T) == typeof(double))
            {
                double* il = (double*)left;
                double* ir = (double*)right;
                uint length = (uint)(ir - il) + 1;
                
                Debug.Assert(Avx2VectorizedSort.DoubleConfig.Unroll >= 1);
                Debug.Assert(Avx2VectorizedSort.DoubleConfig.Unroll <= 12);
                
                if (length < Avx2VectorizedSort.DoubleConfig.SmallSortThresholdElements)
                {
                    BitonicSort.Sort(il, (int)length);
                    return;
                }
                
                var depthLimit = 2 * FloorLog2PlusOne(length);
                var buffer = stackalloc byte[Avx2VectorizedSort.DoubleConfig.PartitionTempSizeInBytes];
                var sorter = new Avx2VectorizedSort(
                    il,
                    ir,
                    buffer,
                    Avx2VectorizedSort.Int32Config.PartitionTempSizeInBytes
                );
                sorter.sort(il, ir, double.MinValue, double.MaxValue, Avx2VectorizedSort.DoubleConfig.REALIGN_BOTH, depthLimit);
                return;
            }            

            throw new NotSupportedException($"The current type {typeof(T).Name} is not supported by this method.");

        }
    }
}

