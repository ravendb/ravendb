using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Voron.Util.PFor;

public static unsafe class BitPacking
{
    public static int UnpackSegmented(byte* inputBuf, int count, uint* outputBuf, uint bit)
    {
        int i = 0;
        var fullSize = (int)bit * Vector256<byte>.Count;
        var read = 0;
        for (; i + 256 < count; i++)
        {
            var cur = outputBuf + i;
            SimdBitPacking<NoTransform>.Unpack256(0, inputBuf + read, cur, bit);
            read += fullSize;
        }
        for (; i + 32 < count; i += 32)
        {
            var cur = outputBuf + i;
            ScalarBitPacking.Unpack32((int*)(inputBuf+read), (int*)cur, (int)bit);
            read += (int)bit * 4;
        }
        if (i < count)
        {
            var inputTempBuffer = stackalloc int[32];
            var outputTempBuffer = stackalloc int[32];
            var sizeInBytes = ((count - i) * bit + 7) / 8;
            Unsafe.CopyBlock(inputTempBuffer, inputBuf + read, (uint)sizeInBytes);
            ScalarBitPacking.Unpack32(inputTempBuffer, outputTempBuffer, (int)bit);
            Unsafe.CopyBlock(outputBuf + i, outputTempBuffer, (uint)(count - i) * sizeof(int));
            read += (int)sizeInBytes;
        }
        return read;
    }

    public static int PackSegmented( uint* inputBuf, int length, byte* outputBuf, uint bit)
    {
        var fullSize = (int)bit * Vector256<byte>.Count;
        int i = 0;
        var written = 0;
        for (; i + 256 < length; i += 256)
        {
            var cur = inputBuf + i;
            SimdBitPacking<NoTransform>.Pack256(0, cur, outputBuf + written, bit);
            written += fullSize;
        }
        for (; i + 32 < length;i+=32)
        {
            var cur = inputBuf + i;
            ScalarBitPacking.Pack32((int*)cur, (int*)(outputBuf + written), (int)bit);
            written += (int)bit * 4; 
        }
        if(i < length)
        {
            var inputTempBuffer = stackalloc int[32];
            var outputTempBuffer = stackalloc int[32];
            Unsafe.CopyBlock(inputTempBuffer, inputBuf + i, (uint)(length - i) * sizeof(uint));
            ScalarBitPacking.Pack32(inputTempBuffer, outputTempBuffer, (int)bit);
            var sizeInBytes = ((length - i) * bit + 7) / 8;
            Unsafe.CopyBlock(outputBuf + written, outputTempBuffer, (uint)sizeInBytes);
            written += (int)sizeInBytes;
        }
        return written;
    }

    public static int RequireSizeSegmented(int len, int bits)
    {
        Debug.Assert(bits is >= 0 and <= 32);
        var (full, partial) = Math.DivRem(len * bits, 8);
        return full + (partial > 0 ? 1 : 0);
    }
}
