using System;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Sparrow.Server.Utils.VxSort
{
    // We will use type erasure to ensure that we can create specific variants of this same algorithm.
    public static unsafe partial class Sort
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int FloorLog2(uint n)
        {
            return 31 - BitOperations.LeadingZeroCount(n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int FloorLog2PlusOne(uint n)
        {
            return FloorLog2(n) + 1;
        }

        public static void Run<T>([NotNull] T[] array) where T : unmanaged
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            if (AdvInstructionSet.X86.IsSupportedAvx256 == false)
            {
                MemoryExtensions.Sort(array.AsSpan());
                return;
            }

            fixed (T* arrayPtr = array)
            {
                T* left = arrayPtr;
                T* right = arrayPtr + array.Length - 1;
                Run(left, right);
            }
        }

        public static void Run<T>([NotNull] Span<T> array) where T : unmanaged
        {
            if (array == Span<T>.Empty)
                throw new ArgumentNullException(nameof(array));

            if (AdvInstructionSet.X86.IsSupportedAvx256 == false)
            {
                MemoryExtensions.Sort(array);
                return;
            }

            // TODO: Improve this.
            fixed (T* arrayPtr = array)
            {
                T* left = arrayPtr;
                T* right = arrayPtr + array.Length - 1;
                Run(left, right);
            }
        }

        public static void Run<T>(T* start, int count) where T : unmanaged
        {
            if (start == null)
                throw new ArgumentNullException(nameof(start));

            if (AdvInstructionSet.X86.IsSupportedAvx256 == false)
            {
                MemoryExtensions.Sort(new Span<T>(start, count));
                return;
            }

            Run(start, start + count - 1);
        }
    }
}
