using System;
using Sparrow.Global;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using System.Runtime.CompilerServices;
#if NET7_0_OR_GREATER
using System.Runtime.Intrinsics;
#endif

namespace Sparrow.Compression
{
    internal sealed unsafe class LZ4
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
        private const int FASTLOOP_SAFE_DISTANCE = 64;
        private const int MATCH_SAFEGUARD_DISTANCE = (2 * COPYLENGTH) - MINMATCH; /* == 12 */

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

        private const uint TABLE_TYPE_CLEARED = 0;
        private const uint TABLE_TYPE_BY_U16 = 1;
        private const uint TABLE_TYPE_BY_U32 = 2;

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
        private struct LZ4_stream_t_internal
        {
            public fixed int hashTable[HASH_SIZE_U32];
            public uint dictSize;
            public uint currentOffset;
            public byte* dictionary;
            public uint tableType;
        }

        [ThreadStatic]
        private static LZ4_stream_t_internal* _cachedCtx;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static LZ4_stream_t_internal* GetCachedCtx()
        {
            var ctx = _cachedCtx;
            if (ctx != null) return ctx;
            return AllocateCachedCtx();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static LZ4_stream_t_internal* AllocateCachedCtx()
        {
            var ptr = (LZ4_stream_t_internal*)Sparrow.Utils.NativeMemory.AllocateMemory(
                sizeof(LZ4_stream_t_internal));
            Memory.Set((byte*)ptr, 0, (uint)sizeof(LZ4_stream_t_internal));
            _cachedCtx = ptr;
            return ptr;
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

        private static void LZ4_prepareTable(LZ4_stream_t_internal* ctx, int inputSize, uint tableType)
        {
            if (ctx->tableType != TABLE_TYPE_CLEARED)
            {
                if (ctx->tableType != tableType
                    || (tableType == TABLE_TYPE_BY_U16 && ctx->currentOffset + (uint)inputSize >= 0xFFFFu)
                    || (tableType == TABLE_TYPE_BY_U32 && ctx->currentOffset > 1u << 30)
                    || inputSize >= 4 * Constants.Size.Kilobyte)
                {
                    Memory.Set((byte*)ctx->hashTable, 0, HASH_SIZE_U32 * sizeof(int));
                    ctx->currentOffset = 0;
                    ctx->tableType = TABLE_TYPE_CLEARED;
                }
            }
            if (ctx->currentOffset != 0 && tableType == TABLE_TYPE_BY_U32)
                ctx->currentOffset += 64 * (uint)Constants.Size.Kilobyte;

            ctx->dictionary = null;
            ctx->dictSize = 0;
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

            LZ4_stream_t_internal* ctx = GetCachedCtx();

            if (outputLength >= MaximumOutputLength(inputLength))
            {
                if (inputLength < LZ4_64Klimit)
                {
                    LZ4_prepareTable(ctx, inputLength, TABLE_TYPE_BY_U16);
                    if (ctx->currentOffset != 0)
                        return LZ4_compress_generic<NotLimited, ByU16, NoDict, DictSmall>(ctx, input, output, inputLength, 0, acceleration);
                    else
                        return LZ4_compress_generic<NotLimited, ByU16, NoDict, NoDictIssue>(ctx, input, output, inputLength, 0, acceleration);
                }
                else
                {
                    LZ4_prepareTable(ctx, inputLength, TABLE_TYPE_BY_U32);
                    return LZ4_compress_generic<NotLimited, ByU32, NoDict, NoDictIssue>(ctx, input, output, inputLength, 0, acceleration);
                }
            }
            else
            {
                if (inputLength < LZ4_64Klimit)
                {
                    LZ4_prepareTable(ctx, inputLength, TABLE_TYPE_BY_U16);
                    if (ctx->currentOffset != 0)
                        return LZ4_compress_generic<LimitedOutput, ByU16, NoDict, DictSmall>(ctx, input, output, inputLength, outputLength, acceleration);
                    else
                        return LZ4_compress_generic<LimitedOutput, ByU16, NoDict, NoDictIssue>(ctx, input, output, inputLength, outputLength, acceleration);
                }
                else
                {
                    LZ4_prepareTable(ctx, inputLength, TABLE_TYPE_BY_U32);
                    return LZ4_compress_generic<LimitedOutput, ByU32, NoDict, NoDictIssue>(ctx, input, output, inputLength, outputLength, acceleration);
                }
            }
        }

        /// <summary>Gets maximum the length of the output.</summary>
        /// <param name="size">Length of the input.</param>
        /// <returns>Maximum number of bytes needed for compressed buffer.</returns>
        public static long MaximumOutputLength(long size)
        {
            return size + (size / 255) + 16;
        }

        public static int MaximumOutputLength(int size)
        {
            return checked(size + (size / 255) + 16);
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
                @base = source - ctx->currentOffset;
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

            ctx->currentOffset += (uint)inputSize;
            ctx->tableType = (typeof(TTableType) == typeof(ByU16)) ? TABLE_TYPE_BY_U16 : TABLE_TYPE_BY_U32;

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
                            uint value = *((uint*)forwardIp) * prime4bytes >> ((MINMATCH * 8) - ByU16HashLog);
                            forwardH = (int)value;
                            ((ushort*)ctx->hashTable)[h] = (ushort)(ip - @base);
                        }
                        else if (typeof(TTableType) == typeof(ByU32))
                        {
                            ulong value = (*(ulong*)forwardIp << 24) * prime5bytes >> (64 - ByU32HashLog);
                            forwardH = (int)value;
                            ctx->hashTable[h] = (int)(ip - @base);
                        }
                        else throw new NotSupportedException("TTableType directive is not supported.");
                    }
                    while (((typeof(TDictionaryIssue) == typeof(DictSmall)) ? (match < lowRefLimit) : false) ||
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
                    WildCopy8(op, anchor, (op + litLength));
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
                if (((typeof(TDictionaryIssue) == typeof(DictSmall)) ? (match >= lowRefLimit) : true) && (match + MAX_DISTANCE >= ip) && (*(uint*)(match + refDelta) == *(uint*)(ip)))
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

                Memory.Copy(op, anchor, (uint)lastRun);
                op += lastRun;
            }

            return (int)(op - dest);
        }


        /// <summary>
        /// Count matching bytes between two memory locations.
        /// v1.10.0: Added early exit on first comparison for common short-match case.
        /// </summary>
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

            // v1.10.0: Early exit on first comparison (most common case: short match)
            // This avoids loop setup overhead for the frequent case where matches are short.
            if (pIn < pInLimit - 7)
            {
                ulong diff = *((ulong*)pMatch) ^ *((ulong*)pIn);
                if (diff != 0)
                    return Bits.TrailingZeroesInBytes(diff);
                pIn += sizeof(ulong);
                pMatch += sizeof(ulong);
            }

            // Continue with loop for longer matches
            while (pIn < pInLimit - 7)
            {
                ulong diff = *((ulong*)pMatch) ^ *((ulong*)pIn);
                if (diff == 0)
                {
                    pIn += sizeof(ulong);
                    pMatch += sizeof(ulong);
                    continue;
                }

                pIn += Bits.TrailingZeroesInBytes(diff);
                return (int)(pIn - pStart);
            }

            // Handle remaining bytes (less than 8)
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
                // v1.7.3: Use 4-byte read and 4-byte prime for small hash tables
                // This is faster than 8-byte read for small data compression
                uint value = *((uint*)sequence) * prime4bytes >> ((MINMATCH * 8) - ByU16HashLog);
                return (int)value;
            }
            else if (typeof(TTableType) == typeof(ByU32))
            {
                // v1.7.3: Use 5-byte prime with improved shift calculation for 64-bit
                ulong value = (*(ulong*)sequence << 24) * prime5bytes >> (64 - ByU32HashLog);
                return (int)value;
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

        // v1.7.3: Use 4-byte prime for ByU16 (small hash table), 5-byte prime for ByU32
        private const uint prime4bytes = 2654435761U;
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

        private static readonly int[] dec32table = new int[] { 4, 1, 2, 1, 4, 4, 4, 4 };
        private static readonly int[] dec64table = new int[] { 0, 0, 0, -1, 0, 1, 2, 3 };

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

            // Two-stage shortcut boundaries for safe loop
            byte* shortiend = iend - (typeof(TEndCondition) == typeof(EndOnInputSize) ? 14 : 8) - 2;
            byte* shortoend = oend - (typeof(TEndCondition) == typeof(EndOnInputSize) ? 14 : 8) - 18;

            // Special Cases
            if ((typeof(TEarlyEnd) == typeof(Partial)) && (oexit > oend - MFLIMIT)) oexit = oend - MFLIMIT;
            if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (outputSize == 0))
                return ((inputSize == 1) && (*ip == 0)) ? 0 : -1;
            if ((typeof(TEndCondition) == typeof(EndOnOutputSize)) && (outputSize == 0))
                return (*ip == 0 ? 1 : -1);

            // Main Loop
            while (true)
            {
                int length;
                byte* match;
                byte* cpy;
                byte token;
                int offset;

                /* ============================================================
                 * FAST LOOP: decode sequences while output has >= FASTLOOP_SAFE_DISTANCE bytes remaining.
                 * All wild copies in this path are safe because we have enough slack.
                 * ============================================================ */
                if ((oend - op) >= FASTLOOP_SAFE_DISTANCE &&
                    typeof(TEndCondition) == typeof(EndOnInputSize))
                {
                    token = *ip++;
                    length = token >> ML_BITS;

                    /* --- fast literal copy --- */
                    if (length == RUN_MASK)
                    {
                        long addl = read_variable_length(&ip, iend - RUN_MASK);
                        if (addl < 0) goto _output_error;
                        length += (int)addl;
                        if ((op + length) < op) goto _output_error; /* overflow */
                        if ((ip + length) < ip) goto _output_error; /* overflow */

                        if (op + length > oend - 32 || ip + length > iend - 32)
                            goto safe_literal_copy;

                        WildCopy32(op, ip, op + length);
                        ip += length; op += length;
                    }
                    else if (ip <= iend - 17)
                    {
                        /* Short literal (0..14): copy 16 bytes inline */
#if NET7_0_OR_GREATER
                        if (AdvInstructionSet.IsAcceleratedVector128)
                        {
                            Vector128.Load(ip).Store(op);
                        }
                        else
#endif
                        {
                            *((ulong*)op) = *(ulong*)ip;
                            *((ulong*)(op + 8)) = *(ulong*)(ip + 8);
                        }
                        ip += length; op += length;
                    }
                    else
                    {
                        goto safe_literal_copy;
                    }

                    /* --- get offset --- */
                    offset = *((ushort*)ip); ip += 2;
                    match = op - offset;

                    if ((checkOffset) && (match + dictSize < lowPrefix))
                        goto _output_error;

                    /* --- external dictionary match (fast loop) --- */
                    if ((typeof(TDictionaryType) == typeof(UsingExtDict)) && (match < lowPrefix))
                    {
                        /* get matchlength */
                        length = token & ML_MASK;
                        if (length == ML_MASK)
                        {
                            long addl = read_variable_length(&ip, iend - LASTLITERALS + 1);
                            if (addl < 0) goto _output_error;
                            length += (int)addl;
                        }
                        length += MINMATCH;

                        if (op + length > oend - LASTLITERALS)
                            goto _output_error;

                        if (length <= (int)(lowPrefix - match))
                        {
                            match = dictEnd - (lowPrefix - match);
                            Memory.Move(op, match, length);
                            op += length;
                        }
                        else
                        {
                            int copySize = (int)(lowPrefix - match);
                            Memory.Copy(op, dictEnd - copySize, (uint)copySize);
                            op += copySize;

                            copySize = length - copySize;
                            if (copySize > (int)(op - lowPrefix))
                            {
                                byte* endOfMatch = op + copySize;
                                byte* copyFrom = lowPrefix;
                                while (op < endOfMatch)
                                    *op++ = *copyFrom++;
                            }
                            else
                            {
                                Memory.Copy(op, lowPrefix, (uint)copySize);
                                op += copySize;
                            }
                        }
                        continue;
                    }

                    /* --- get matchlength --- */
                    length = token & ML_MASK;

                    if (length == ML_MASK)
                    {
                        long addl = read_variable_length(&ip, iend - LASTLITERALS + 1);
                        if (addl < 0) goto _output_error;
                        length += (int)addl;
                        length += MINMATCH;
                        if ((op + length) < op) goto _output_error; /* overflow */
                        if (op + length >= oend - FASTLOOP_SAFE_DISTANCE)
                            goto safe_match_copy;
                    }
                    else
                    {
                        length += MINMATCH;
                        if (op + length >= oend - FASTLOOP_SAFE_DISTANCE)
                            goto safe_match_copy;

                        /* Fastpath check: short match with large offset */
                        if ((typeof(TDictionaryType) == typeof(NoDict) || typeof(TDictionaryType) == typeof(WithPrefix64K)) &&
                            match >= lowPrefix)
                        {
                            if (offset >= 8)
                            {
                                /* Copy match (up to 18 bytes: 8 + 8 + 2) */
                                *((ulong*)op) = *(ulong*)match;
                                *((ulong*)(op + 8)) = *(ulong*)(match + 8);
                                *((ushort*)(op + 16)) = *(ushort*)(match + 16);
                                op += length;
                                continue;
                            }
                        }
                    }

                    /* --- copy match within block (fast loop) --- */
                    cpy = op + length;

                    if (offset < 16)
                    {
                        CopyWithOverlap(op, match, cpy, offset);
                    }
                    else
                    {
                        WildCopy32(op, match, cpy);
                    }
                    op = cpy;
                    continue;
                }

                /* ============================================================
                 * SAFE LOOP: decode remaining sequences near end of buffers.
                 * Uses WildCopy8 (max 7-byte overshoot) instead of WildCopy32.
                 * ============================================================ */
                token = *ip++;
                length = token >> ML_BITS;

                // Two-stage shortcut for the most common case
                if (typeof(TEndCondition) == typeof(EndOnInputSize) &&
                    typeof(TDictionaryType) != typeof(UsingExtDict) &&
                    length != RUN_MASK &&
                    ip < shortiend &&
                    op <= shortoend)
                {
                    // Stage 1: Copy literals (up to 16 bytes)
#if NET7_0_OR_GREATER
                    if (AdvInstructionSet.IsAcceleratedVector128)
                    {
                        Vector128.Load(ip).Store(op);
                    }
                    else
#endif
                    {
                        *((ulong*)op) = *(ulong*)ip;
                        *((ulong*)(op + 8)) = *(ulong*)(ip + 8);
                    }
                    op += length;
                    ip += length;

                    // Stage 2: Prepare match info
                    int matchLength = token & ML_MASK;
                    offset = *((ushort*)ip);
                    ip += 2;
                    match = op - offset;

                    if (matchLength != ML_MASK && offset >= 8 && match >= lowPrefix)
                    {
                        *((ulong*)op) = *(ulong*)match;
                        *((ulong*)(op + 8)) = *(ulong*)(match + 8);
                        *((ushort*)(op + 16)) = *(ushort*)(match + 16);
                        op += matchLength + MINMATCH;
                        continue;
                    }

                    length = matchLength;
                    goto _copy_match;
                }

                if (length == RUN_MASK)
                {
                    byte s;
                    do
                    {
                        s = *ip++;
                        length += s;
                    }
                    while (((typeof(TEndCondition) == typeof(EndOnInputSize)) ? ip < iend - RUN_MASK : true) && (s == 255));

                    if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (op + length) < op) goto _output_error;
                    if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (ip + length) < ip) goto _output_error;
                }

            safe_literal_copy:
                // copy literals
                cpy = op + length;
                if (((typeof(TEndCondition) == typeof(EndOnInputSize)) && ((cpy > (typeof(TEarlyEnd) == typeof(Partial) ? oexit : oend - MFLIMIT)) || (ip + length > iend - (2 + 1 + LASTLITERALS))))
                    || ((typeof(TEndCondition) == typeof(EndOnOutputSize)) && (cpy > oend - COPYLENGTH)))
                {
                    if (typeof(TEarlyEnd) == typeof(Partial))
                    {
                        if (cpy > oend)
                            goto _output_error;

                        if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && (ip + length > iend))
                            goto _output_error;
                    }
                    else
                    {
                        if ((typeof(TEndCondition) == typeof(EndOnOutputSize)) && (cpy != oend))
                            goto _output_error;

                        if ((typeof(TEndCondition) == typeof(EndOnInputSize)) && ((ip + length != iend) || (cpy > oend)))
                            goto _output_error;
                    }

                    Memory.Copy(op, ip, (uint)length);
                    ip += length;
                    op += length;
                    if (typeof(TEarlyEnd) != typeof(Partial) || (cpy == oend) || (ip >= (iend - 2)))
                        break;
                }
                else
                {
                    WildCopy8(op, ip, cpy);   /* max 7 bytes overshoot */
                    ip += length; op = cpy;
                }

                /* get offset */
                offset = *((ushort*)ip); ip += sizeof(ushort);
                match = op - offset;
                if ((checkOffset) && (match < lowLimit))
                    goto _output_error;

                /* get matchlength */
                length = token & ML_MASK;

            _copy_match:
                if (length == ML_MASK)
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
                        goto _output_error;
                }

                length += MINMATCH;

            safe_match_copy:
                /* check external dictionary */
                if ((checkOffset) && (match + dictSize < lowPrefix))
                    goto _output_error;

                if ((typeof(TDictionaryType) == typeof(UsingExtDict)) && (match < lowPrefix))
                {
                    if (op + length > oend - LASTLITERALS)
                        goto _output_error;

                    if (length <= (int)(lowPrefix - match))
                    {
                        match = dictEnd - (lowPrefix - match);
                        Memory.Move(op, match, length);
                        op += length;
                    }
                    else
                    {
                        int copySize = (int)(lowPrefix - match);
                        Memory.Copy(op, dictEnd - copySize, (uint)copySize);
                        op += copySize;

                        copySize = length - copySize;
                        if (copySize > (int)(op - lowPrefix))
                        {
                            byte* endOfMatch = op + copySize;
                            byte* copyFrom = lowPrefix;
                            while (op < endOfMatch)
                                *op++ = *copyFrom++;
                        }
                        else
                        {
                            Memory.Copy(op, lowPrefix, (uint)copySize);
                            op += copySize;
                        }
                    }
                    continue;
                }

                /* copy repeated sequence */
                cpy = op + length;
                int matchOffset = (int)(op - match);

                // Partial decoding near end
                if (typeof(TEarlyEnd) == typeof(Partial) && (cpy > oend - MATCH_SAFEGUARD_DISTANCE))
                {
                    int mlen = length < (int)(oend - op) ? length : (int)(oend - op);
                    byte* matchEnd = match + mlen;
                    byte* copyEnd = op + mlen;
                    if (matchEnd > op)
                    {
                        while (op < copyEnd)
                            *op++ = *match++;
                    }
                    else
                    {
                        Memory.Copy(op, match, (uint)mlen);
                        op = copyEnd;
                    }
                    if (op == oend) break;
                    continue;
                }

                if (matchOffset < 8)
                {
                    // Small offsets need special handling
                    if (cpy > oend - 12)
                    {
                        if (cpy > oend - LASTLITERALS)
                            goto _output_error;

                        while (op < cpy)
                            *op++ = *match++;
                    }
                    else
                    {
                        CopySmallOffset(op, match, cpy, matchOffset);
                        op = cpy;
                    }
                    continue;
                }

                // Normal offset (>= 8): no overlap concerns
                *((ulong*)op) = *(ulong*)match;
                op += sizeof(ulong);
                match += sizeof(ulong);

                if (cpy > oend - MATCH_SAFEGUARD_DISTANCE)
                {
                    if (cpy > oend - LASTLITERALS)
                        goto _output_error;

                    byte* oCopyLimit = oend - (COPYLENGTH - 1);
                    if (op < oCopyLimit)
                    {
                        WildCopy8(op, match, oCopyLimit);
                        match += oCopyLimit - op;
                        op = oCopyLimit;
                    }

                    while (op < cpy)
                        *op++ = *match++;
                }
                else
                {
                    *((ulong*)op) = *(ulong*)match;
                    if (length > 16)
                        WildCopy8(op + 8, match + 8, cpy);
                }

                op = cpy;
            }

            /* end of decoding */
            if (typeof(TEndCondition) == typeof(EndOnInputSize))
                return (int)(op - dest);
            else
                return (int)(ip - source);

            /* Overflow error detected */
            _output_error:
            return (int)(-(ip - source)) - 1;
        }

        /// <summary>
        /// Reads a variable-length integer from the stream (used for extended literal/match lengths).
        /// Returns the accumulated value, or -1 on error (read past limit).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long read_variable_length(byte** ip, byte* ilimit)
        {
            long length = 0;
            byte s;
            if (*ip >= ilimit) return -1;
            do
            {
                s = *(*ip);
                (*ip)++;
                length += s;
                if (*ip > ilimit) return -1;
            }
            while (s == 255);
            return length;
        }

        /// <summary>
        /// WildCopy8 - Copy 8 bytes per iteration. Max overshoot: 7 bytes.
        /// Used in safe-path decompression and compression literal copies.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WildCopy8(byte* dest, byte* src, byte* destEnd)
        {
            do
            {
                *((ulong*)dest) = *(ulong*)src;
                dest += 8;
                src += 8;
            }
            while (dest < destEnd);
        }

        /// <summary>
        /// WildCopy32 - Copy 32 bytes per iteration. Max overshoot: 31 bytes.
        /// Used only in the fast decompression loop where FASTLOOP_SAFE_DISTANCE guarantees slack.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WildCopy32(byte* dest, byte* src, byte* destEnd)
        {
            do
            {
#if NET7_0_OR_GREATER
                if (AdvInstructionSet.IsAcceleratedVector128)
                {
                    Vector128.Load(src).Store(dest);
                    Vector128.Load(src + 16).Store(dest + 16);
                }
                else
#endif
                {
                    *((ulong*)dest) = *(ulong*)src;
                    *((ulong*)(dest + 8)) = *(ulong*)(src + 8);
                    *((ulong*)(dest + 16)) = *(ulong*)(src + 16);
                    *((ulong*)(dest + 24)) = *(ulong*)(src + 24);
                }
                dest += 32;
                src += 32;
            }
            while (dest < destEnd);
        }

        /// <summary>
        /// Copy match data for offsets &lt; 16 where source and destination overlap.
        /// For offset &lt; 8: pattern-based or table-based copy (avoids load-blocked-by-store hazards).
        /// For offset 8-15: 8-byte copy + WildCopy8 (safe because each 8-byte read is from already-written data).
        /// WildCopy32 cannot be used here because its 16-byte Vector128 loads would read unwritten output.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void CopyWithOverlap(byte* op, byte* match, byte* cpy, int offset)
        {
            if (offset < 8)
            {
                CopySmallOffset(op, match, cpy, offset);
            }
            else
            {
                // Offset 8-15: straight 8-byte copy establishes the base,
                // then WildCopy8 (8-byte steps) is overlap-safe since offset >= 8.
                *((ulong*)op) = *(ulong*)match;
                op += 8;
                match += 8;
                if (op < cpy)
                    WildCopy8(op, match, cpy);
            }
        }

        /// <summary>
        /// Optimized copy for small offsets (1, 2, 4).
        /// For these offsets, we build an 8-byte pattern and use pattern repetition
        /// instead of overlapped byte-by-byte copies. This avoids load-blocked-by-store
        /// hazards that occur when reading from recently written memory locations.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void CopySmallOffset(byte* op, byte* match, byte* cpy, int offset)
        {
            ulong pattern;
            switch (offset)
            {
                case 1:
                    // RLE - single byte repeated
                    pattern = (ulong)(*match) * 0x0101010101010101UL;
                    break;
                case 2:
                    // Two-byte pattern repeated
                    ulong s = *(ushort*)match;
                    pattern = s | (s << 16) | (s << 32) | (s << 48);
                    break;
                case 4:
                    // Four-byte pattern repeated
                    ulong u = *(uint*)match;
                    pattern = u | (u << 32);
                    break;
                default:
                    // For offsets 3, 5, 6, 7: use generic table-based copy
                    // This path handles irregular patterns that don't tile evenly
                    int dec64 = dec64table[offset];
                    op[0] = match[0];
                    op[1] = match[1];
                    op[2] = match[2];
                    op[3] = match[3];
                    match += dec32table[offset];
                    *((uint*)(op + 4)) = *(uint*)match;
                    op += 8;
                    match -= dec64;
                    // Continue with WildCopy8 for remaining bytes
                    if (op < cpy)
                    {
                        WildCopy8(op, match, cpy);
                    }
                    return;
            }

            // Pattern repetition loop for offsets 1, 2, 4
            do
            {
                *((ulong*)op) = pattern;
                op += 8;
            }
            while (op < cpy);
        }
    }
}
