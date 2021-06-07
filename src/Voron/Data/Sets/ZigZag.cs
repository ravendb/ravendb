using System;

namespace Voron.Data.Sets
{
    public static class ZigZag
    {
        public static int Encode(Span<byte> buffer, long value)
        {
            ulong uv = (ulong)((value << 1) ^ (value >> 63));
            return Encode7Bits(buffer, uv);
        }
            
        public static int Encode7Bits(Span<byte> buffer, ulong uv)
        {
            var len = 0;
            while (uv > 0x7Fu)
            {
                buffer[len++] = ((byte)((uint)uv | ~0x7Fu));
                uv >>= 7;
            }
            buffer[len++] = ((byte)uv);
            return len;
        }
        
        public static long Decode(Span<byte> buffer)
        {
            ulong result = Decode7Bits(buffer, out _);
            return UnZag(result);
        }

        private static long UnZag(ulong result)
        {
            return ((result & 1) != 0 ? (long)(result >> 1) ^ -1 : (long)(result >> 1));
        }

        public static int SizeOfBoth(Span<byte> buffer)
        {
            Decode7Bits(buffer, out var fst);
            Decode7Bits(buffer.Slice(fst), out var snd);
            return fst + snd;
        }
        
        public static (long, long) DecodeBoth(Span<byte> buffer)
        {
            ulong result = Decode7Bits(buffer, out var used);
            var one =  UnZag(result);
            result = Decode7Bits(buffer.Slice(used), out _);
            var two =  UnZag(result);
            return (one, two);
        }

            
        public static ulong Decode7Bits(Span<byte> buffer, out int used)
        {
            ulong result = 0;
            byte byteReadJustNow;
            var length = 0;

            const int maxBytesWithoutOverflow = 9;
            for (int shift = 0; shift < maxBytesWithoutOverflow * 7; shift += 7)
            {
                byteReadJustNow = buffer[length++];
                result |= (byteReadJustNow & 0x7Ful) << shift;

                if (byteReadJustNow <= 0x7Fu)
                {
                    used = length;
                    return result;
                }
            }

            byteReadJustNow = buffer[length++];
            if (byteReadJustNow > 0b_1u)
            {
                throw new ArgumentOutOfRangeException("result", "Bad var int value");
            }

            result |= (ulong)byteReadJustNow << (maxBytesWithoutOverflow * 7);
            used = length;
            return result;
        }
    }
}
