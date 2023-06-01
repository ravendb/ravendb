using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.Intrinsics;
using Voron.Util.Simd;

namespace Voron.Util.PFor;

// Based on: https://github.com/lemire/simdcomp

public unsafe struct SimdBitPacking<TSimdDiff>
    where TSimdDiff : struct, ISimdTransform
{
    public static int PackSmall(uint initValue, Span<uint> inputBuf, Span<byte> outputBuf, uint bit, TSimdDiff transform)
    {
        fixed (uint* i = inputBuf)
        {
            fixed (byte* o = outputBuf)
            {
                return PackSmall(initValue, i, inputBuf.Length, o, bit, transform);
            }
        }
    }

    public static int UnpackSmall(uint initValue, Span<byte> inputBuf, int length, Span<uint> outputBuf, uint bit, TSimdDiff transform)
    {
        fixed (byte* i = inputBuf)
        {
            fixed (uint* o = outputBuf)
            {
                return UnpackSmall(initValue, i, length, o, bit, transform);
            }
        }
    }

    public static int UnpackSmall(uint initValue, byte* inputBuf, int length, uint* outputBuf, uint bit, TSimdDiff tranform)
    {
        if (length == 0)
            return 0;
        switch (bit)
        {
            case 0:
                new Span<uint>(outputBuf, length).Fill(initValue);
                return length * sizeof(uint);
            case 32:
                new Span<uint>(inputBuf, length).CopyTo(new Span<uint>(outputBuf, length));
                return length * sizeof(uint);
            case < 0 or > 32:
                Debug.Fail("Should never be reached");
                throw new InvalidOperationException("Wrong number of of bits used in unpacking: " + bit);
        }

        var offset = Vector256.Create(initValue);
        var mask = Vector256.Create((1u << (int)bit) - 1u);
        var inWordPos = 0u;
        var input = (Vector256<uint>*)inputBuf;
        var output = (Vector256<uint>*)outputBuf;
        var p = *input++;
        for (int i = 0; i < length / 8; i++)
        {
            var answer = Vector256.ShiftRightLogical(p, (int)inWordPos);
            var remainingBits = sizeof(uint) * 8 - inWordPos;
            if (bit < remainingBits)
            {
                inWordPos += bit;
            }
            else
            {
                p = *input++;
                answer |= Vector256.ShiftLeft(p, (int)remainingBits);
                inWordPos = bit - remainingBits;
            }
            answer &= mask;
            *output++ = tranform.Decode(answer, ref offset);
        }
        var outputUint = (uint*)output;
        if (length % 8 != 0)
        {
            var answer = Vector256.ShiftRightLogical(p, (int)inWordPos);
            var remainingBits = sizeof(uint) * 8 - inWordPos;
            if (bit >= remainingBits)
            {
                p = *input;
                answer |= Vector256.ShiftLeft(p, (int)remainingBits);
            }
            answer &= mask;
            tranform.Decode(answer, ref offset).Store(outputUint);
            outputUint += length;
        }
        return (int)((byte*)outputBuf - (byte*)outputUint);
    }

    public static int PackSmall(uint initValue, uint* inputBuf, int length, byte* outputBuf, uint bit, TSimdDiff tranform = default)
    {
        if (length == 0)
            return 0;
        switch (bit)
        {
            case 32:
                new Span<uint>(inputBuf, length).CopyTo(new Span<uint>(outputBuf, length));
                return length * sizeof(uint);
            case < 0 or > 32:
                Debug.Fail("Should never be reached");
                throw new InvalidOperationException("Wrong number of of bits used in unpacking: " + bit);
        }

        var offset = Vector256.Create(initValue);
        var input = (Vector256<uint>*)inputBuf;
        var output = (Vector256<uint>*)outputBuf;
        var p = Vector256<uint>.Zero;
        uint inWordPos = 0;
        for (int i = 0; i < length / 8; i++)
        {
            var cur = *input++;
            var v = tranform.Encode(cur, ref offset);
            p |= Vector256.ShiftLeft(v, (int)inWordPos);
            uint remainingBits = sizeof(uint) * 8 - inWordPos;
            if (bit < remainingBits)
            {
                inWordPos += bit;
            }
            else
            {
                *output++ = p;
                p = Vector256.ShiftRightLogical(v, (int)remainingBits);
                inWordPos = bit - remainingBits;
            }
        }
        if (length % 8 != 0)
        {
            uint* buffer = stackalloc uint[8];
            int i = 0;
            for (; i < length % 8; i++)
            {
                buffer[i] = inputBuf[length / 8 * 8 + i];
            }
            for (; i < 8; i++)
            {
                buffer[i] = 0;
            }

            var v = tranform.Encode(Vector256.Load(buffer), ref offset);
            p |= Vector256.ShiftLeft(v, (int)inWordPos);
            var firstPass = sizeof(uint) * 8 - inWordPos;
            if (bit < firstPass)
            {
                inWordPos += bit;
            }
            else
            {
                *output++ = p;
                p = Vector256.ShiftRightLogical(v, (int)firstPass);
                inWordPos = bit - firstPass;
            }
        }
        if (inWordPos != 0)
        {
            *output++ = p;
        }

        return (int)((byte*)output - outputBuf);
    }

    public static void Unpack256(uint initValue, Span<byte> inputBuf, Span<uint> output, uint bit, TSimdDiff transform = default)
    {
        fixed (byte* input = inputBuf)
        {
            fixed (uint* outputB = output)
            {
                Unpack256(initValue, input, outputB, bit, transform);
            }
        }
    }

    public static void Unpack256(uint initValue, byte* inputBuf, uint* outputBuf,
        uint bit, TSimdDiff transform = default)
    {
        var input = (Vector256<uint>*)inputBuf;
        var output = (Vector256<uint>*)outputBuf;
        var initOffset = Vector256.Create(initValue);
        switch (bit)
        {
            case 0:
                SimdPacking<TSimdDiff>.iunpackFOR0(initOffset, input, output, transform);
                break;

            case 1:
                SimdPacking<TSimdDiff>.iunpackFOR1(initOffset, input, output, transform);
                break;

            case 2:
                SimdPacking<TSimdDiff>.iunpackFOR2(initOffset, input, output, transform);
                break;

            case 3:
                SimdPacking<TSimdDiff>.iunpackFOR3(initOffset, input, output, transform);
                break;

            case 4:
                SimdPacking<TSimdDiff>.iunpackFOR4(initOffset, input, output, transform);
                break;

            case 5:
                SimdPacking<TSimdDiff>.iunpackFOR5(initOffset, input, output, transform);
                break;

            case 6:
                SimdPacking<TSimdDiff>.iunpackFOR6(initOffset, input, output, transform);
                break;

            case 7:
                SimdPacking<TSimdDiff>.iunpackFOR7(initOffset, input, output, transform);
                break;

            case 8:
                SimdPacking<TSimdDiff>.iunpackFOR8(initOffset, input, output, transform);
                break;

            case 9:
                SimdPacking<TSimdDiff>.iunpackFOR9(initOffset, input, output, transform);
                break;

            case 10:
                SimdPacking<TSimdDiff>.iunpackFOR10(initOffset, input, output, transform);
                break;

            case 11:
                SimdPacking<TSimdDiff>.iunpackFOR11(initOffset, input, output, transform);
                break;

            case 12:
                SimdPacking<TSimdDiff>.iunpackFOR12(initOffset, input, output, transform);
                break;

            case 13:
                SimdPacking<TSimdDiff>.iunpackFOR13(initOffset, input, output, transform);
                break;

            case 14:
                SimdPacking<TSimdDiff>.iunpackFOR14(initOffset, input, output, transform);
                break;

            case 15:
                SimdPacking<TSimdDiff>.iunpackFOR15(initOffset, input, output, transform);
                break;

            case 16:
                SimdPacking<TSimdDiff>.iunpackFOR16(initOffset, input, output, transform);
                break;

            case 17:
                SimdPacking<TSimdDiff>.iunpackFOR17(initOffset, input, output, transform);
                break;

            case 18:
                SimdPacking<TSimdDiff>.iunpackFOR18(initOffset, input, output, transform);
                break;

            case 19:
                SimdPacking<TSimdDiff>.iunpackFOR19(initOffset, input, output, transform);
                break;

            case 20:
                SimdPacking<TSimdDiff>.iunpackFOR20(initOffset, input, output, transform);
                break;

            case 21:
                SimdPacking<TSimdDiff>.iunpackFOR21(initOffset, input, output, transform);
                break;

            case 22:
                SimdPacking<TSimdDiff>.iunpackFOR22(initOffset, input, output, transform);
                break;

            case 23:
                SimdPacking<TSimdDiff>.iunpackFOR23(initOffset, input, output, transform);
                break;

            case 24:
                SimdPacking<TSimdDiff>.iunpackFOR24(initOffset, input, output, transform);
                break;

            case 25:
                SimdPacking<TSimdDiff>.iunpackFOR25(initOffset, input, output, transform);
                break;

            case 26:
                SimdPacking<TSimdDiff>.iunpackFOR26(initOffset, input, output, transform);
                break;

            case 27:
                SimdPacking<TSimdDiff>.iunpackFOR27(initOffset, input, output, transform);
                break;

            case 28:
                SimdPacking<TSimdDiff>.iunpackFOR28(initOffset, input, output, transform);
                break;

            case 29:
                SimdPacking<TSimdDiff>.iunpackFOR29(initOffset, input, output, transform);
                break;

            case 30:
                SimdPacking<TSimdDiff>.iunpackFOR30(initOffset, input, output, transform);
                break;

            case 31:
                SimdPacking<TSimdDiff>.iunpackFOR31(initOffset, input, output, transform);
                break;

            case 32:
                SimdPacking<TSimdDiff>.iunpackFOR32(initOffset, input, output, transform);
                break;

            default:
                throw new InvalidOperationException("Wrong number of of bits used in unpacking: " + bit);
        }

    }

    public static void Pack256(uint initValue, Span<uint> inputBuf, Span<byte> output, uint bit, TSimdDiff transform = default)
    {
        fixed (uint* input = inputBuf)
        {
            fixed (byte* outputB = output)
            {
                Pack256(initValue, input, outputB, bit, transform);
            }
        }
    }

    public static void Pack256(uint initValue, uint* inputBuf, byte* outputBuf, uint bit, TSimdDiff transform = default)
    {
        var initOffset = Vector256.Create(initValue);
        var input = (Vector256<uint>*)inputBuf;
        var output = (Vector256<uint>*)outputBuf;
        switch (bit)
        {
            case 0:
                SimdPacking<TSimdDiff>.ipackFOR0(initOffset, input, output, transform);
                break;

            case 1:
                SimdPacking<TSimdDiff>.ipackFOR1(initOffset, input, output, transform);
                break;

            case 2:
                SimdPacking<TSimdDiff>.ipackFOR2(initOffset, input, output, transform);
                break;

            case 3:
                SimdPacking<TSimdDiff>.ipackFOR3(initOffset, input, output, transform);
                break;

            case 4:
                SimdPacking<TSimdDiff>.ipackFOR4(initOffset, input, output, transform);
                break;

            case 5:
                SimdPacking<TSimdDiff>.ipackFOR5(initOffset, input, output, transform);
                break;

            case 6:
                SimdPacking<TSimdDiff>.ipackFOR6(initOffset, input, output, transform);
                break;

            case 7:
                SimdPacking<TSimdDiff>.ipackFOR7(initOffset, input, output, transform);
                break;

            case 8:
                SimdPacking<TSimdDiff>.ipackFOR8(initOffset, input, output, transform);
                break;

            case 9:
                SimdPacking<TSimdDiff>.ipackFOR9(initOffset, input, output, transform);
                break;

            case 10:
                SimdPacking<TSimdDiff>.ipackFOR10(initOffset, input, output, transform);
                break;

            case 11:
                SimdPacking<TSimdDiff>.ipackFOR11(initOffset, input, output, transform);
                break;

            case 12:
                SimdPacking<TSimdDiff>.ipackFOR12(initOffset, input, output, transform);
                break;

            case 13:
                SimdPacking<TSimdDiff>.ipackFOR13(initOffset, input, output, transform);
                break;

            case 14:
                SimdPacking<TSimdDiff>.ipackFOR14(initOffset, input, output, transform);
                break;

            case 15:
                SimdPacking<TSimdDiff>.ipackFOR15(initOffset, input, output, transform);
                break;

            case 16:
                SimdPacking<TSimdDiff>.ipackFOR16(initOffset, input, output, transform);
                break;

            case 17:
                SimdPacking<TSimdDiff>.ipackFOR17(initOffset, input, output, transform);
                break;

            case 18:
                SimdPacking<TSimdDiff>.ipackFOR18(initOffset, input, output, transform);
                break;

            case 19:
                SimdPacking<TSimdDiff>.ipackFOR19(initOffset, input, output, transform);
                break;

            case 20:
                SimdPacking<TSimdDiff>.ipackFOR20(initOffset, input, output, transform);
                break;

            case 21:
                SimdPacking<TSimdDiff>.ipackFOR21(initOffset, input, output, transform);
                break;

            case 22:
                SimdPacking<TSimdDiff>.ipackFOR22(initOffset, input, output, transform);
                break;

            case 23:
                SimdPacking<TSimdDiff>.ipackFOR23(initOffset, input, output, transform);
                break;

            case 24:
                SimdPacking<TSimdDiff>.ipackFOR24(initOffset, input, output, transform);
                break;

            case 25:
                SimdPacking<TSimdDiff>.ipackFOR25(initOffset, input, output, transform);
                break;

            case 26:
                SimdPacking<TSimdDiff>.ipackFOR26(initOffset, input, output, transform);
                break;

            case 27:
                SimdPacking<TSimdDiff>.ipackFOR27(initOffset, input, output, transform);
                break;

            case 28:
                SimdPacking<TSimdDiff>.ipackFOR28(initOffset, input, output, transform);
                break;

            case 29:
                SimdPacking<TSimdDiff>.ipackFOR29(initOffset, input, output, transform);
                break;

            case 30:
                SimdPacking<TSimdDiff>.ipackFOR30(initOffset, input, output, transform);
                break;

            case 31:
                SimdPacking<TSimdDiff>.ipackFOR31(initOffset, input, output, transform);
                break;

            case 32:
                SimdPacking<TSimdDiff>.ipackFOR32(initOffset, input, output, transform);
                break;

            default:
                throw new InvalidOperationException("Wrong number of of bits used in unpacking: " + bit);
        }
    }

    public static uint FindMaxBits(uint initValue, Span<uint> data)
    {
        fixed (uint* p = data)
            return FindMaxBits(initValue, p, data.Length);
    }
    public static uint FindMaxBits(uint initValue, uint* data, int len)
    {
        var dataVec = (Vector256<uint>*)data;
        var prev = Vector256.Create(initValue);
        var acc = Vector256<uint>.Zero;
        int i = 0;
        TSimdDiff simdDiff = default;
        for (; i + Vector256<uint>.Count <= len; i += Vector256<uint>.Count)
        {
            var cur = *dataVec++;
            acc |= simdDiff.Encode(cur, ref prev);
        }
        var accumulators = stackalloc uint[Vector256<uint>.Count];
        acc.Store(accumulators);
        var prevScalar = prev.GetElement(7);
        uint scalarAcc = 0;
        for (; i < len; i++)
        {
            scalarAcc |= data[i] - prevScalar;
            prevScalar = data[i];
        }
        for (int j = 0; j < 8; j++)
        {
            scalarAcc |= accumulators[j];
        }
        return 32 - (uint)BitOperations.LeadingZeroCount(scalarAcc);
    }
}
