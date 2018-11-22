using System;
using Sparrow.Global;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using System.Runtime.CompilerServices;

namespace Sparrow.Compression
{
    public unsafe class LZ4
    {
        public const int ACCELERATION_DEFAULT = 1;

        private const int COPYLENGTH = 8;
        private const int LASTLITERALS = 5;
        private const int MINMATCH = 4;
        private const int MFLIMIT = COPYLENGTH + MINMATCH;
        private const int LZ4_minLength = MFLIMIT + 1;

        private const int MAXD_LOG = 16;
        private const int MAX_DISTANCE = ((1 << MAXD_LOG) - 1);

        private const int LZ4_64Klimit = (64 * Constants.Size.Kilobyte) + (MFLIMIT - 1);
        private const int LZ4_skipTrigger = 6;  // Increase this value ==> compression run slower on incompressible data

        private const byte ML_BITS = 4;
        private const byte ML_MASK = ((1 << ML_BITS) - 1);
        private const byte RUN_BITS = (8 - ML_BITS);
        private const byte RUN_MASK = ((1 << RUN_BITS) - 1);

        private const uint LZ4_MAX_INPUT_SIZE = 0x7E000000;  /* 2 113 929 216 bytes */

        /// <summary>
        /// LZ4_MEMORY_USAGE :
        /// Memory usage formula : N->2^N Bytes(examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
        /// Increasing memory usage improves compression ratio
        /// Reduced memory usage can improve speed, due to cache effect
        /// Default value is 14, for 16KB, which nicely fits into Intel x86 L1 cache
        /// </summary>
        private const int LZ4_MEMORY_USAGE = 14;
        private const int LZ4_HASHLOG = LZ4_MEMORY_USAGE - 2;
        private const int HASH_SIZE_U32 = 1 << LZ4_HASHLOG;
        private const int MAX_INPUT_LENGTH_PER_SEGMENT = int.MaxValue/2;

        private interface ILimitedOutputDirective { };
        private struct NotLimited : ILimitedOutputDirective { };
        private struct LimitedOutput : ILimitedOutputDirective { };

        private interface IDictionaryTypeDirective { };
        private struct NoDict : IDictionaryTypeDirective { };
        private struct WithPrefix64K : IDictionaryTypeDirective { };
        private struct UsingExtDict : IDictionaryTypeDirective { };

        private interface IDictionaryIssueDirective { };
        private struct NoDictIssue : IDictionaryIssueDirective { };
        private struct DictSmall : IDictionaryIssueDirective { };

        private interface ITableTypeDirective { };
        private struct ByU32 : ITableTypeDirective { };
        private struct ByU16 : ITableTypeDirective { };

        private interface IEndConditionDirective { };
        private struct EndOnOutputSize : IEndConditionDirective { };
        private struct EndOnInputSize : IEndConditionDirective { };

        private interface IEarlyEndDirective { };
        private struct Full : IEarlyEndDirective { };
        private struct Partial : IEarlyEndDirective { };

        [StructLayout(LayoutKind.Sequential)]
        protected struct LZ4_stream_t_internal
        {
            public fixed int hashTable[HASH_SIZE_U32];
            public uint dictSize;
            public uint currentOffset;
            public byte* dictionary;
            public uint initCheck;
        }

        public static long Encode64LongBuffer(
            byte* input,
            byte* output,
            long inputLength,
            long outputLength,
            int acceleration = ACCELERATION_DEFAULT)
        {
            // LZ4 can handle a bit less then 2GB. we will handle the compression/decompression devided to parts for above 1GB inputs
            if (inputLength < MAX_INPUT_LENGTH_PER_SEGMENT && outputLength < MAX_INPUT_LENGTH_PER_SEGMENT)
            {
                return Encode64(input, output, (int)inputLength, (int)outputLength, acceleration);
            }

            long totalOutputSize = 0;
            long readPosition = 0;
            while (readPosition < inputLength)
            {
                int partInputLength = MAX_INPUT_LENGTH_PER_SEGMENT;
                if (readPosition + partInputLength > inputLength)
                    partInputLength = (int)(inputLength - readPosition);

                int remaining = (outputLength - totalOutputSize) > int.MaxValue ? int.MaxValue : (int)(outputLength - totalOutputSize);

                totalOutputSize += Encode64(input + readPosition, output + totalOutputSize, partInputLength, remaining, acceleration);

                readPosition += partInputLength;
            }

            return totalOutputSize;
        }

        public static int Encode64(
                byte* input,
                byte* output,
                int inputLength,
                int outputLength,
                int acceleration = ACCELERATION_DEFAULT)
        {
            if (acceleration < 1)
                acceleration = ACCELERATION_DEFAULT;

            LZ4_stream_t_internal ctx = new LZ4_stream_t_internal();

            if (outputLength >= MaximumOutputLength(inputLength))
            {
                if (inputLength < LZ4_64Klimit)
                    return LZ4_compress_generic<NotLimited, ByU16, NoDict, NoDictIssue>(&ctx, input, output, inputLength, 0, acceleration);
                else
                    return LZ4_compress_generic<NotLimited, ByU32, NoDict, NoDictIssue>(&ctx, input, output, inputLength, 0, acceleration);
            }
            else
            {
                if (inputLength < LZ4_64Klimit)
                    return LZ4_compress_generic<LimitedOutput, ByU16, NoDict, NoDictIssue>(&ctx, input, output, inputLength, outputLength, acceleration);
                else
                    return LZ4_compress_generic<LimitedOutput, ByU32, NoDict, NoDictIssue>(&ctx, input, output, inputLength, outputLength, acceleration);
            }
        }

        /// <summary>Gets maximum the length of the output.</summary>
        /// <param name="size">Length of the input.</param>
        /// <returns>Maximum number of bytes needed for compressed buffer.</returns>
        public static long MaximumOutputLength(long size)
        {
            return size + (size / 255) + 16;
        }

        private static int LZ4_compress_generic<TLimited, TTableType, TDictionaryType, TDictionaryIssue>(LZ4_stream_t_internal* dictPtr, byte* source, byte* dest, int inputSize, int maxOutputSize, int acceleration)
            where TLimited : ILimitedOutputDirective
            where TTableType : ITableTypeDirective
            where TDictionaryType : IDictionaryTypeDirective
            where TDictionaryIssue : IDictionaryIssueDirective
        {

            LZ4_stream_t_internal* ctx = dictPtr;

            byte* op = dest;
            byte* ip = source;
            byte* anchor = source;

            byte* dictionary = ctx->dictionary;
            byte* dictEnd = dictionary + ctx->dictSize;
            byte* lowRefLimit = ip - ctx->dictSize;

            long dictDelta = (long)dictEnd - (long)source;

            byte* iend = ip + inputSize;
            byte* mflimit = iend - MFLIMIT;
            byte* matchlimit = iend - LASTLITERALS;

            byte* olimit = op + maxOutputSize;

            // Init conditions
            if (inputSize > LZ4_MAX_INPUT_SIZE) return 0;   // Unsupported input size, too large (or negative)

            byte* @base;
            byte* lowLimit;

            if (typeof(TDictionaryType) == typeof(NoDict))
            {
                @base = source;
                lowLimit = source;
            }
            else if (typeof(TDictionaryType) == typeof(WithPrefix64K))
            {
                @base = source - ctx->currentOffset;
                lowLimit = source - ctx->dictSize;
            }
            else if (typeof(TDictionaryType) == typeof(UsingExtDict))
            {
                @base = source - ctx->currentOffset;
                lowLimit = source;
            }
            else throw new NotSupportedException("Unsupported IDictionaryTypeDirective.");

            if ((typeof(TTableType) == typeof(ByU16)) && (inputSize >= LZ4_64Klimit)) // Size too large (not within 64K limit)
                return 0;

            if (inputSize < LZ4_minLength) // Input too small, no compression (all literals)
                goto _last_literals;

            // First Byte
            LZ4_putPosition<TTableType>(ip, ctx, @base);
            ip++;
            int forwardH = LZ4_hashPosition<TTableType>(ip);

            // Main Loop
            long refDelta = 0;
            for (;;)
            {
                byte* match;
                {
                    byte* forwardIp = ip;

                    int step = 1;
                    int searchMatchNb = acceleration << LZ4_skipTrigger;

                    do
                    {
                        int h = forwardH;
                        ip = forwardIp;
                        forwardIp += step;
                        step = (searchMatchNb++ >> LZ4_skipTrigger);

                        if (forwardIp > mflimit)
                            goto _last_literals;

                        match = LZ4_getPositionOnHash<TTableType>(h, ctx, @base);
                        if (typeof(TDictionaryType) == typeof(UsingExtDict))
                        {
                            if (match < source)
                            {
                                refDelta = dictDelta;
                                lowLimit = dictionary;
                            }
                            else
                            {
                                refDelta = 0;
                                lowLimit = source;
                            }
                        }

                        if (typeof(TTableType) == typeof(ByU16))
                        {
                            ulong value = *((ulong*)forwardIp) * prime5bytes >> (40 - ByU16HashLog);
                            forwardH = (int)(value & ByU16HashMask);
                            ((ushort*)ctx->hashTable)[h] = (ushort)(ip - @base);
                        }
                        else if (typeof(TTableType) == typeof(ByU32))
                        {
                            ulong value = (*((ulong*)forwardIp) * prime5bytes >> (40 - ByU32HashLog));
                            forwardH = (int)(value & ByU32HashMask);
                            ctx->hashTable[h] = (int)(ip - @base);
                        }
                        else throw new NotSupportedException("TTableType directive is not supported.");
                    }
                    while (((typeof(TDictionaryType) == typeof(DictSmall)) ? (match < lowRefLimit) : false) ||
                           ((typeof(TTableType) == typeof(ByU16)) ? false : (match + MAX_DISTANCE < ip)) ||
                           (*(uint*)(match + refDelta) != *((uint*)ip)));
                }

                // Catch up
                while ((ip > anchor) && (match + refDelta > lowLimit) && (ip[-1] == match[refDelta - 1]))
                {
                    ip--;
                    match--;
                }


                // Encode Literal length
                byte* token;
                {
                    int litLength = (int)(ip - anchor);
                    token = op++;

                    if ((typeof(TLimited) == typeof(LimitedOutput)) && (op + litLength + (2 + 1 + LASTLITERALS) + (litLength / 255) > olimit))
                        return 0;   /* Check output limit */

                    if (litLength >= RUN_MASK)
                    {
                        int len = litLength - RUN_MASK;
                        *token = RUN_MASK << ML_BITS;

                        for (; len >= 255; len -= 255)
                            *op++ = 255;

                        *op++ = (byte)len;
                    }
                    else
                    {
                        *token = (byte)(litLength << ML_BITS);
                    }

                    /* Copy Literals */
                    WildCopy(op, anchor, (op + litLength));
                    op += litLength;
                }

                _next_match:

                // Encode Offset                
                *((ushort*)op) = (ushort)(ip - match);
                op += sizeof(ushort);

                // Encode MatchLength
                {
                    int matchLength;

                    if ((typeof(TDictionaryType) == typeof(UsingExtDict)) && (lowLimit == dictionary))
                    {
                        match += refDelta;

                        byte* limit = ip + (dictEnd - match);
                        if (limit > matchlimit) limit = matchlimit;
                        matchLength = LZ4_count(ip + MINMATCH, match + MINMATCH, limit);
                        ip += MINMATCH + matchLength;
                        if (ip == limit)
                        {
                            int more = LZ4_count(ip, source, matchlimit);
                            matchLength += more;
                            ip += more;
                        }
                    }
                    else
                    {
                        matchLength = LZ4_count(ip + MINMATCH, match + MINMATCH, matchlimit);
                        ip += MINMATCH + matchLength;
                    }

                    if ((typeof(TLimited) == typeof(LimitedOutput)) && ((op + (1 + LASTLITERALS) + (matchLength >> 8)) > olimit))
                        return 0;    /* Check output limit */

                    if (matchLength >= ML_MASK)
                    {
                        *token += ML_MASK;
                        matchLength -= ML_MASK;

                        for (; matchLength >= 510; matchLength -= 510)
                        {
                            *(ushort*)op = (255 << 8 | 255);
                            op += sizeof(ushort);
                        }

                        if (matchLength >= 255)
                        {
                            matchLength -= 255;
                            *op++ = 255;
                        }

                        *op++ = (byte)matchLength;
                    }
                    else
                    {
                        *token += (byte)(matchLength);
                    }
                }


                anchor = ip;

                // Test end of chunk
                if (ip > mflimit) break;

                // Fill table
                LZ4_putPosition<TTableType>(ip - 2, ctx, @base);

                /* Test next position */
                match = LZ4_getPosition<TTableType>(ip, ctx, @base);
                if (typeof(TDictionaryType) == typeof(UsingExtDict))
                {
                    if (match < source)
                    {
                        refDelta = dictDelta;
                        lowLimit = dictionary;
                    }
                    else
                    {
                        refDelta = 0;
                        lowLimit = source;
                    }
                }

                LZ4_putPosition<TTableType>(ip, ctx, @base);
                if (((typeof(TDictionaryType) == typeof(DictSmall)) ? (match >= lowRefLimit) : true) && (match + MAX_DISTANCE >= ip) && (*(uint*)(match + refDelta) == *(uint*)(ip)))
                {
                    token = op++; *token = 0;
                    goto _next_match;
                }

                /* Prepare next loop */
                forwardH = LZ4_hashPosition<TTableType>(++ip);
            }

            _last_literals:

            /* Encode Last Literals */
            {
                int lastRun = (int)(iend - anchor);
                if ((typeof(TLimited) == typeof(LimitedOutput)) && ((op - dest) + lastRun + 1 + ((lastRun + 255 - RUN_MASK) / 255) > maxOutputSize))
                    return 0;   // Check output limit;

                if (lastRun >= RUN_MASK)
                {
                    int accumulator = lastRun - RUN_MASK;
                    *op++ = RUN_MASK << ML_BITS;

                    for (; accumulator >= 255; accumulator -= 255)
                        *op++ = 255;

                    *op++ = (byte)accumulator;
                }
                else
                {
                    *op++ = (byte)(lastRun << ML_BITS);
                }

                Unsafe.CopyBlock(op, anchor, (uint)lastRun);
                op += lastRun;
            }

            return (int)(op - dest);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int LZ4_count(byte* pInPtr, byte* pMatchPtr, byte* pInLimitPtr)
        {
            // JIT: We make local copies of the parameters because the JIT will not be able to figure out yet that it can safely inline
            //      the method cloning the parameters. As the arguments are modified the JIT will not be able to inline it.
            //      This wont be needed anymore when https://github.com/dotnet/coreclr/issues/6014 is resolved.
            byte* pIn = pInPtr;
            byte* pMatch = pMatchPtr;
            byte* pInLimit = pInLimitPtr;

            byte* pStart = pIn;

            while (pIn < pInLimit - (sizeof(ulong) - 1))
            {
                ulong diff = *((ulong*)pMatch) ^ *((ulong*)pIn);
                if (diff == 0)
                {
                    pIn += sizeof(ulong);
                    pMatch += sizeof(ulong);
                    continue;
                }

                pIn += Bits.TrailingZeroes(diff);
                return (int)(pIn - pStart);
            }

            if ((pIn < (pInLimit - 3)) && (*((uint*)pMatch) == *((uint*)(pIn)))) { pIn += sizeof(uint); pMatch += sizeof(uint); }
            if ((pIn < (pInLimit - 1)) && (*((ushort*)pMatch) == *((ushort*)pIn))) { pIn += sizeof(ushort); pMatch += sizeof(ushort); }
            if ((pIn < pInLimit) && (*pMatch == *pIn)) pIn++;

            return (int)(pIn - pStart);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void LZ4_putPosition<TTableType>(byte* p, LZ4_stream_t_internal* ctx, byte* srcBase)
            where TTableType : ITableTypeDirective
        {
            int h = LZ4_hashPosition<TTableType>(p);
            LZ4_putPositionOnHash<TTableType>(p, h, ctx, srcBase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte* LZ4_getPosition<TTableType>(byte* p, LZ4_stream_t_internal* ctx, byte* srcBase)
            where TTableType : ITableTypeDirective
        {
            int h = LZ4_hashPosition<TTableType>(p);
            return LZ4_getPositionOnHash<TTableType>(h, ctx, srcBase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void LZ4_putPositionOnHash<TTableType>(byte* p, int h, LZ4_stream_t_internal* ctx, byte* srcBase)
            where TTableType : ITableTypeDirective
        {
            if (typeof(TTableType) == typeof(ByU32))
                ctx->hashTable[h] = (int)(p - srcBase);
            else if (typeof(TTableType) == typeof(ByU16))
                ((ushort*)ctx->hashTable)[h] = (ushort)(p - srcBase);
            else
                ThrowException(new NotSupportedException("TTableType directive is not supported."));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte* LZ4_getPositionOnHash<TTableType>(int h, LZ4_stream_t_internal* ctx, byte* srcBase)
            where TTableType : ITableTypeDirective
        {
            if (typeof(TTableType) == typeof(ByU32))
                return srcBase + ctx->hashTable[h];
            else if (typeof(TTableType) == typeof(ByU16))
                return srcBase + ((ushort*)ctx->hashTable)[h];

            ThrowException(new NotSupportedException("TTableType directive is not supported."));
            return default(byte*);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int LZ4_hashPosition<TTableType>(byte* sequence)
            where TTableType : ITableTypeDirective
        {
            if (typeof(TTableType) == typeof(ByU16))
            {
                ulong value = *((ulong*)sequence) * prime5bytes >> (40 - ByU16HashLog);
                return (int)(value & ByU16HashMask);
            }
            else if (typeof(TTableType) == typeof(ByU32))
            {
                ulong value = (*((ulong*)sequence) * prime5bytes >> (40 - ByU32HashLog));
                return (int)(value & ByU32HashMask);
            }

            return ThrowException<int>(new NotSupportedException("TTableType directive is not supported."));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ThrowException(Exception e)
        {
            throw e;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static TResult ThrowException<TResult>(Exception e)
        {
            throw e;
        }

        private const int ByU16HashLog = LZ4_HASHLOG + 1;
        private const ulong ByU16HashMask = (1 << ByU16HashLog) - 1;

        private const int ByU32HashLog = LZ4_HASHLOG;
        private const ulong ByU32HashMask = (1 << ByU32HashLog) - 1;

        private const ulong prime5bytes = 889523592379UL;

        public static long Decode64LongBuffers(
            byte* input,
            long inputLength,
            byte* output,
            long outputLength,
            bool knownOutputLength)
        {
            // here we get a single compressed segment or multiple segments
            // we can read the segments only for a known size of output
            if (outputLength < MAX_INPUT_LENGTH_PER_SEGMENT && inputLength < MAX_INPUT_LENGTH_PER_SEGMENT)
            {
                return Decode64(input, (int)inputLength, output, (int)outputLength, knownOutputLength);
            }

            long totalReadSize = 0;
            long totalWriteSize = 0;
            while (totalReadSize < inputLength)
            {
                int partInputLength = int.MaxValue;
                if (totalReadSize + partInputLength > inputLength)
                    partInputLength = (int) (inputLength - totalReadSize);

                int partOutputLength = MAX_INPUT_LENGTH_PER_SEGMENT;
                if (totalWriteSize + partOutputLength > outputLength)
                {
                    partOutputLength = checked((int)(outputLength - totalWriteSize));
                }
                totalReadSize += Decode64(input + totalReadSize, partInputLength, output + totalWriteSize, partOutputLength, false);

                totalWriteSize += MAX_INPUT_LENGTH_PER_SEGMENT;
            }

            return totalReadSize;
        }

        public static int Decode64(
            byte* input,
            int inputLength,
            byte* output,
            int outputLength,
            bool knownOutputLength)
        {
            if (knownOutputLength)
            {
                var length = LZ4_decompress_generic<EndOnInputSize, Full, NoDict>(input, output, inputLength, outputLength, 0, output, null, 0);
                if (length != outputLength)
                    ThrowException(new ArgumentException("LZ4 block is corrupted, or invalid length has been given."));
                return outputLength;
            }
            else
            {
                var length = LZ4_decompress_generic<EndOnOutputSize, Full, WithPrefix64K>(input, output, inputLength, outputLength, 0, output - (64 * Constants.Size.Kilobyte), null, 64 * Constants.Size.Kilobyte);
                if (length < 0)
                    ThrowException(new ArgumentException("LZ4 block is corrupted, or invalid length has been given."));

                return length;
            }
        }

        private readonly static int[] dec32table = new int[] { 4, 1, 2, 1, 4, 4, 4, 4 };
        private readonly static int[] dec64table = new int[] { 0, 0, 0, -1, 0, 1, 2, 3 };

        private static int LZ4_decompress_generic<TEndCondition, TEarlyEnd, TDictionaryType>(byte* source, byte* dest, int inputSize, int outputSize, int targetOutputSize, byte* lowPrefix, byte* dictStart, int dictSize)
            where TEndCondition : IEndConditionDirective
            where TEarlyEnd : IEarlyEndDirective
            where TDictionaryType : IDictionaryTypeDirective
        {
            /* Local Variables */
            byte* ip = source;
            byte* iend = ip + inputSize;

            byte* op = dest;
            byte* oend = op + outputSize;

            byte* oexit = op + targetOutputSize;
            byte* lowLimit = lowPrefix - dictSize;

            byte* dictEnd = dictStart + dictSize;

            bool checkOffset = ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (dictSize < 64 * Constants.Size.Kilobyte));

            // Special Cases
            if ((typeof(TEarlyEnd) == typeof(Partial)) && (oexit > oend - MFLIMIT)) oexit = oend - MFLIMIT;                          // targetOutputSize too high => decode everything
            if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (outputSize == 0))
                return ((inputSize == 1) && (*ip == 0)) ? 0 : -1;  // Empty output buffer
            if ((typeof(TEndCondition) == typeof(EndOnOutputSize)) && (outputSize == 0))
                return (*ip == 0 ? 1 : -1);

            // Main Loop
            while (true)
            {
                int length;

                /* get literal length */
                byte token = *ip++;
                if ((length = (token >> ML_BITS)) == RUN_MASK)
                {
                    byte s;
                    do
                    {
                        s = *ip++;
                        length += s;
                    }
                    while (((typeof(TEndCondition) == typeof(EndOnInputSize)) ? ip < iend - RUN_MASK : true) && (s == 255));

                    if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (op + length) < op) goto _output_error;   /* overflow detection */
                    if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (ip + length) < ip) goto _output_error;   /* overflow detection */
                }

                // copy literals
                byte* cpy = op + length;
                if (((typeof(TEndCondition) == typeof(EndOnInputSize)) && ((cpy > (typeof(TEarlyEnd) == typeof(Partial) ? oexit : oend - MFLIMIT)) || (ip + length > iend - (2 + 1 + LASTLITERALS))))
                    || ((typeof(TEndCondition) == typeof(EndOnOutputSize)) && (cpy > oend - COPYLENGTH)))
                {
                    if (typeof(TEarlyEnd) == typeof(Partial))
                    {
                        if (cpy > oend)
                            goto _output_error;                           /* Error : write attempt beyond end of output buffer */

                        if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (ip + length > iend))
                            goto _output_error;   /* Error : read attempt beyond end of input buffer */
                    }
                    else
                    {
                        if ((typeof(TEndCondition) == typeof(EndOnOutputSize)) && (cpy != oend))
                            goto _output_error;       /* Error : block decoding must stop exactly there */

                        if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && ((ip + length != iend) || (cpy > oend)))
                            goto _output_error;   /* Error : input must be consumed */
                    }

                    Unsafe.CopyBlock(op, ip, (uint)length);
                    ip += length;
                    op += length;
                    break;     /* Necessarily EOF, due to parsing restrictions */
                }

                WildCopy(op, ip, cpy);
                ip += length; op = cpy;

                /* get offset */
                byte* match = cpy - *((ushort*)ip); ip += sizeof(ushort);
                if ((checkOffset) && (match < lowLimit))
                    goto _output_error;   /* Error : offset outside destination buffer */

                /* get matchlength */
                if ((length = (token & ML_MASK)) == ML_MASK)
                {
                    byte s;
                    do
                    {
                        if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (ip > iend - LASTLITERALS))
                            goto _output_error;

                        s = *ip++;
                        length += s;
                    }
                    while (s == 255);

                    if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (op + length) < op)
                        goto _output_error;   /* overflow detection */
                }

                length += MINMATCH;

                /* check external dictionary */
                if ((typeof(TDictionaryType) == typeof(UsingExtDict)) && (match < lowPrefix))
                {
                    if (op + length > oend - LASTLITERALS)
                        goto _output_error;   /* doesn't respect parsing restriction */

                    if (length <= (int)(lowPrefix - match))
                    {
                        /* match can be copied as a single segment from external dictionary */
                        match = dictEnd - (lowPrefix - match);                        
                        Memory.Move(op, match, length); 
                        op += length;
                    }
                    else
                    {
                        /* match encompass external dictionary and current segment */
                        int copySize = (int)(lowPrefix - match);
                        Unsafe.CopyBlock(op, dictEnd - copySize, (uint)copySize);
                        op += copySize;

                        copySize = length - copySize;
                        if (copySize > (int)(op - lowPrefix))   /* overlap within current segment */
                        {
                            byte* endOfMatch = op + copySize;
                            byte* copyFrom = lowPrefix;
                            while (op < endOfMatch)
                                *op++ = *copyFrom++;
                        }
                        else
                        {
                            Unsafe.CopyBlock(op, lowPrefix, (uint)copySize);                            
                            op += copySize;
                        }
                    }
                    continue;
                }

                /* copy repeated sequence */
                cpy = op + length;
                if ((op - match) < 8)
                {
                    int dec64 = dec64table[op - match];
                    op[0] = match[0];
                    op[1] = match[1];
                    op[2] = match[2];
                    op[3] = match[3];

                    match += dec32table[op - match];
                    *((uint*)(op + 4)) = *(uint*)match;
                    op += 8;
                    match -= dec64;
                }
                else
                {
                    *((ulong*)op) = *(ulong*)match;
                    op += sizeof(ulong);
                    match += sizeof(ulong);
                }

                if (cpy > oend - 12)
                {
                    if (cpy > oend - LASTLITERALS)
                        goto _output_error;    /* Error : last LASTLITERALS bytes must be literals */

                    if (op < oend - 8)
                    {
                        WildCopy(op, match, (oend - 8));
                        match += (oend - 8) - op;
                        op = oend - 8;
                    }

                    while (op < cpy)
                        *op++ = *match++;
                }
                else
                {
                    WildCopy(op, match, cpy);
                }

                op = cpy;   /* correction */
            }

            /* end of decoding */
            if (typeof(TEndCondition) == typeof(EndOnInputSize))
                return (int)(op - dest);     /* Nb of output bytes decoded */
            else
                return (int)(ip - source);   /* Nb of input bytes read */

            /* Overflow error detected */
            _output_error:
            return (int)(-(ip - source)) - 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WildCopy(byte* dest, byte* src, byte* destEnd)
        {                  
            do
            {
                ((ulong*)dest)[0] = ((ulong*)src)[0];
                if (dest + 1 * sizeof(ulong) >= destEnd)
                    goto Return;

                ((ulong*)dest)[1] = ((ulong*)src)[1];
                if (dest + 2 * sizeof(ulong) >= destEnd)
                    goto Return;

                ((ulong*)dest)[2] = ((ulong*)src)[2];
                if (dest + 3 * sizeof(ulong) >= destEnd)
                    goto Return;

                ((ulong*)dest)[3] = ((ulong*)src)[3];

                dest += 4 * sizeof(ulong);
                src +=  4 * sizeof(ulong);
            }
            while (dest < destEnd);

            Return:
            return;
        }
    }
}
