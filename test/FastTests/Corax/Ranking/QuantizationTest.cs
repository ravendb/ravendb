using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Corax.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Ranking;

public class QuantizationTest : RavenTestBase
{
    public QuantizationTest(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanEncodeAndDecodeEveryNumberUnderShort()
    {
        for (short value = 1; value < short.MaxValue; ++value)
        {
            var quantized = EntryIdEncodings.FrequencyQuantization(value);
            var quantizedByClassic = EntryIdEncodings.FrequencyQuantizationReference(value);
            Assert.Equal(quantizedByClassic, quantized);
            Assert.True(quantized <= byte.MaxValue); //In range

            var decoded = EntryIdEncodings.FrequencyReconstructionFromQuantization(quantized);
            Assert.False(decoded <= 0);
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax | RavenTestCategory.Intrinsics)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(1)]
    public void Vector256InstructionCorrectlyIgnoresFrequency(int size)
    {
        var random = new Random(2337);
        var ids = Enumerable.Range(0, size).Select(i => (long)random.Next(31_111, 59_999)).ToArray();

        var idsWithShifted = ids.Select(i => i << 10).ToArray();
        var idsWithShiftedCopy = idsWithShifted.ToArray();

        EntryIdEncodings.DecodeAndDiscardFrequencyClassic(idsWithShiftedCopy.AsSpan(), size);
        EntryIdEncodings.DecodeAndDiscardFrequencyVector256(idsWithShifted.AsSpan(), size);

        Assert.Equal(ids, idsWithShifted);
        Assert.Equal(idsWithShifted, idsWithShiftedCopy);
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax | RavenTestCategory.Intrinsics, RavenIntrinsics.ArmBase)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(1)]
    public void AdvSimdInstructionCorrectlyIgnoresFrequency(int size)
    {
        var random = new Random(2337);
        var ids = Enumerable.Range(0, size).Select(i => (long)random.Next(31_111, 59_999)).ToArray();

        var idsWithShifted = ids.Select(i => i << 10).ToArray();
        var idsWithShiftedCopy = idsWithShifted.ToArray();

        EntryIdEncodings.DecodeAndDiscardFrequencyClassic(idsWithShiftedCopy.AsSpan(), size);
        EntryIdEncodings.DecodeAndDiscardFrequencyNeon(idsWithShifted.AsSpan(), size);

        Assert.Equal(ids, idsWithShifted);
        Assert.Equal(idsWithShifted, idsWithShiftedCopy);
    }
    
    // We were trying to improve the performance of the EntryIdEncodings's functions by using SIMD, but we found that the fixed keyword used to obtain a pointer from a Span was causing a significant
    // performance overhead that nullified any gains from using SIMD. We attempted to load a vector using Unsafe.AsPointer, but this approach was not safe because we're sometimes using
    // a buffer from a managed pointer, which could be moved by the garbage collector to a different location. After some research, we discovered a third way to load a vector that is both GC safe
    // and does not require pinning memory. Although this approach is not documented, we added two tests to this function to ensure that any internal changes to the method would trigger a notification,
    // since debugging such issues can be extremely challenging.
    [Fact]
    public void CanSafelyReadVectorFromManagedMemory2()
    {
        var buffer = new long[4];

        ref var firstElement = ref buffer[0];
        
        Assert.True(VectorsShouldBeEqualAfterGc(ref firstElement));
        
        
        bool VectorsShouldBeEqualAfterGc(ref long ptr)
        {
            var vecO = Unsafe.ReadUnaligned<Vector256<long>>(ref Unsafe.As<long, byte>(ref ptr));

            ref byte address = ref Unsafe.As<long, byte>(ref ptr);

            GC.Collect(2, GCCollectionMode.Aggressive, true, compacting: true);
            var vec1 = Unsafe.ReadUnaligned<Vector256<long>>(ref address);

            return vecO.Equals(vec1);
        }
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Corax | RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
    public unsafe void CanSafelyReadVectorFromManagedMemory()
    {
        var toDelete = new List<int[]>();
        for (int i = 0; i < 1000; i++)
        {
            var array = new int[256];
            array.AsSpan().Fill(int.MaxValue);
            toDelete.Add(array);
        }

        var buffer = new int[256];
        buffer.AsSpan().Fill(int.MaxValue);
        ref var b = ref buffer[0];
        ref var currentPtr = ref Unsafe.Add(ref b, 0);
        toDelete = null;

        var firstPtr = Unsafe.AsPointer(ref b);
        GC.Collect(2, GCCollectionMode.Aggressive, true, compacting: true);

        var innerBuffer = Vector256.LoadUnsafe(ref currentPtr);
        var shiftRightLogical = Avx2.ShiftRightLogical(innerBuffer, 10);
        Vector256.StoreUnsafe(shiftRightLogical, ref currentPtr);


        for (int i = 0; i < 8; ++i)
            Assert.Equal(int.MaxValue >> 10, buffer[i]);
    }
}
