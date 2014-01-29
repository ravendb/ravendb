#region LZ4 original

/*
   LZ4 - Fast LZ compression algorithm
   Copyright (C) 2011-2012, Yann Collet.
   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

	   * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
	   * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - LZ4 homepage : http://fastcompression.blogspot.com/p/lz4.html
   - LZ4 source repository : http://code.google.com/p/lz4/
*/

#endregion

#region LZ4 port

/*
Copyright (c) 2013, Milosz Krajewski
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided
that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions
  and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions
  and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#endregion

using System;
using System.Runtime.InteropServices;
using Voron.Impl;

// ReSharper disable InconsistentNaming
// ReSharper disable TooWideLocalVariableScope
// ReSharper disable JoinDeclarationAndInitializer



namespace Voron.Util
{
	public unsafe class LZ4 : IDisposable
	{
		private readonly ushort* _hashtable64K;
		private readonly uint* _hashtable;

		public LZ4()
		{
			_hashtable64K = (ushort*)Marshal.AllocHGlobal(HASH64K_TABLESIZE*sizeof (ushort)).ToPointer();
			_hashtable = (uint*)Marshal.AllocHGlobal(HASH_TABLESIZE * sizeof(uint)).ToPointer();
		}

		public int Encode64(
				byte* input,
				byte* output,
				int inputLength,
				int outputLength)
		{
			if (inputLength < LZ4_64KLIMIT)
			{
				NativeMethods.memset((byte*) _hashtable64K, 0, HASH64K_TABLESIZE*sizeof (ushort));
				return LZ4_compress64kCtx_64(_hashtable64K, input, output, inputLength, outputLength);
			}

			NativeMethods.memset((byte*)_hashtable, 0, HASH_TABLESIZE * sizeof(uint));
			return LZ4_compressCtx_64(_hashtable, input, output, inputLength, outputLength);
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
				var length = LZ4_uncompress_64(input, output, outputLength);
				if (length != inputLength)
					throw new ArgumentException("LZ4 block is corrupted, or invalid length has been given.");
				return outputLength;
			}
			else
			{
				var length = LZ4_uncompress_unknownOutputSize_64(input, output, inputLength, outputLength);
				if (length < 0)
					throw new ArgumentException("LZ4 block is corrupted, or invalid length has been given.");
				return length;
			}
		}

		#region configuration

		/// <summary>
		/// Memory usage formula : N->2^N Bytes (examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
		/// Increasing memory usage improves compression ratio
		/// Reduced memory usage can improve speed, due to cache effect
		/// Default value is 14, for 16KB, which nicely fits into Intel x86 L1 cache
		/// </summary>
		private const int MEMORY_USAGE = 14;

		/// <summary>
		/// Decreasing this value will make the algorithm skip faster data segments considered "incompressible"
		/// This may decrease compression ratio dramatically, but will be faster on incompressible data
		/// Increasing this value will make the algorithm search more before declaring a segment "incompressible"
		/// This could improve compression a bit, but will be slower on incompressible data
		/// The default value (6) is recommended
		/// </summary>
		private const int NOTCOMPRESSIBLE_DETECTIONLEVEL = 6;

		#endregion

		#region consts

		private const int MINMATCH = 4;
#pragma warning disable 162
		private const int SKIPSTRENGTH = NOTCOMPRESSIBLE_DETECTIONLEVEL > 2 ? NOTCOMPRESSIBLE_DETECTIONLEVEL : 2;
#pragma warning restore 162
		private const int COPYLENGTH = 8;
		private const int LASTLITERALS = 5;
		private const int MFLIMIT = COPYLENGTH + MINMATCH;
		private const int MINLENGTH = MFLIMIT + 1;
		private const int MAXD_LOG = 16;
		private const int MAXD = 1 << MAXD_LOG;
		private const int MAXD_MASK = MAXD - 1;
		private const int MAX_DISTANCE = (1 << MAXD_LOG) - 1;
		private const int ML_BITS = 4;
		private const int ML_MASK = (1 << ML_BITS) - 1;
		private const int RUN_BITS = 8 - ML_BITS;
		private const int RUN_MASK = (1 << RUN_BITS) - 1;
		private const int STEPSIZE_64 = 8;
		private const int STEPSIZE_32 = 4;

		private const int LZ4_64KLIMIT = (1 << 16) + (MFLIMIT - 1);

		private const int HASH_LOG = MEMORY_USAGE - 2;
		private const int HASH_TABLESIZE = 1 << HASH_LOG;
		private const int HASH_ADJUST = (MINMATCH * 8) - HASH_LOG;

		private const int HASH64K_LOG = HASH_LOG + 1;
		private const int HASH64K_TABLESIZE = 1 << HASH64K_LOG;
		private const int HASH64K_ADJUST = (MINMATCH * 8) - HASH64K_LOG;

		private const int HASHHC_LOG = MAXD_LOG - 1;
		private const int HASHHC_TABLESIZE = 1 << HASHHC_LOG;
		private const int HASHHC_ADJUST = (MINMATCH * 8) - HASHHC_LOG;
		//private const int HASHHC_MASK = HASHHC_TABLESIZE - 1;

		private static readonly int[] DECODER_TABLE_32 = new[] { 0, 3, 2, 3, 0, 0, 0, 0 };
		private static readonly int[] DECODER_TABLE_64 = new[] { 0, 0, 0, -1, 0, 1, 2, 3 };

		private static readonly int[] DEBRUIJN_TABLE_32 = new[] {
			0, 0, 3, 0, 3, 1, 3, 0, 3, 2, 2, 1, 3, 2, 0, 1,
			3, 3, 1, 2, 2, 2, 2, 0, 3, 1, 2, 0, 1, 0, 1, 1
		};

		private static readonly int[] DEBRUIJN_TABLE_64 = new[] {
			0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7,
			0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7,
			7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6,
			7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7
		};

		private const int MAX_NB_ATTEMPTS = 256;
		private const int OPTIMAL_ML = (ML_MASK - 1) + MINMATCH;

		#endregion

		#region public interface (common)

		/// <summary>Gets maximum the length of the output.</summary>
		/// <param name="inputLength">Length of the input.</param>
		/// <returns>Maximum number of bytes needed for compressed buffer.</returns>
		public static int MaximumOutputLength(int inputLength)
		{
			return inputLength + (inputLength / 255) + 16;
		}

		#endregion

		#region LZ4_compressCtx_64

		private static unsafe int LZ4_compressCtx_64(
			uint* hash_table,
			byte* src,
			byte* dst,
			int src_len,
			int dst_maxlen)
		{
			byte* _p;

			fixed (int* debruijn64 = &DEBRUIJN_TABLE_64[0])
			{
				// r93
				var src_p = src;
				var src_base = src_p;
				var src_anchor = src_p;
				var src_end = src_p + src_len;
				var src_mflimit = src_end - MFLIMIT;

				var dst_p = dst;
				var dst_end = dst_p + dst_maxlen;

				var src_LASTLITERALS = src_end - LASTLITERALS;
				var src_LASTLITERALS_1 = src_LASTLITERALS - 1;

				var src_LASTLITERALS_3 = src_LASTLITERALS - 3;
				var src_LASTLITERALS_STEPSIZE_1 = src_LASTLITERALS - (STEPSIZE_64 - 1);
				var dst_LASTLITERALS_1 = dst_end - (1 + LASTLITERALS);
				var dst_LASTLITERALS_3 = dst_end - (2 + 1 + LASTLITERALS);

				int length;
				uint h, h_fwd;

				// Init
				if (src_len < MINLENGTH) goto _last_literals;

				// First Byte
				hash_table[((((*(uint*)(src_p))) * 2654435761u) >> HASH_ADJUST)] = (uint)(src_p - src_base);
				src_p++;
				h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> HASH_ADJUST);

				// Main Loop
				while (true)
				{
					var findMatchAttempts = (1 << SKIPSTRENGTH) + 3;
					var src_p_fwd = src_p;
					byte* src_ref;
					byte* dst_token;

					// Find a match
					do
					{
						h = h_fwd;
						var step = findMatchAttempts++ >> SKIPSTRENGTH;
						src_p = src_p_fwd;
						src_p_fwd = src_p + step;

						if (src_p_fwd > src_mflimit) goto _last_literals;

						h_fwd = ((((*(uint*)(src_p_fwd))) * 2654435761u) >> HASH_ADJUST);
						src_ref = src_base + hash_table[h];
						hash_table[h] = (uint)(src_p - src_base);
					} while ((src_ref < src_p - MAX_DISTANCE) || ((*(uint*)(src_ref)) != (*(uint*)(src_p))));

					// Catch up
					while ((src_p > src_anchor) && (src_ref > src) && (src_p[-1] == src_ref[-1]))
					{
						src_p--;
						src_ref--;
					}

					// Encode Literal length
					length = (int)(src_p - src_anchor);
					dst_token = dst_p++;

					if (dst_p + length + (length >> 8) > dst_LASTLITERALS_3) return 0; // Check output limit

					if (length >= RUN_MASK)
					{
						var len = length - RUN_MASK;
						*dst_token = (RUN_MASK << ML_BITS);
						if (len > 254)
						{
							do
							{
								*dst_p++ = 255;
								len -= 255;
							} while (len > 254);
							*dst_p++ = (byte)len;
							BlockCopy(src_anchor, dst_p, (length));
							dst_p += length;
							goto _next_match;
						}
						*dst_p++ = (byte)len;
					}
					else
					{
						*dst_token = (byte)(length << ML_BITS);
					}

					// Copy Literals
					_p = dst_p + (length);
					{
						do
						{
							*(ulong*)dst_p = *(ulong*)src_anchor;
							dst_p += 8;
							src_anchor += 8;
						} while (dst_p < _p);
					}
					dst_p = _p;

				_next_match:

					// Encode Offset
					*(ushort*)dst_p = (ushort)(src_p - src_ref);
					dst_p += 2;

					// Start Counting
					src_p += MINMATCH;
					src_ref += MINMATCH; // MinMatch already verified
					src_anchor = src_p;

					while (src_p < src_LASTLITERALS_STEPSIZE_1)
					{
						var diff = (*(long*)(src_ref)) ^ (*(long*)(src_p));
						if (diff == 0)
						{
							src_p += STEPSIZE_64;
							src_ref += STEPSIZE_64;
							continue;
						}
						src_p += debruijn64[(((ulong)((diff) & -(diff)) * 0x0218A392CDABBD3FL)) >> 58];
						goto _endCount;
					}

					if ((src_p < src_LASTLITERALS_3) && ((*(uint*)(src_ref)) == (*(uint*)(src_p))))
					{
						src_p += 4;
						src_ref += 4;
					}
					if ((src_p < src_LASTLITERALS_1) && ((*(ushort*)(src_ref)) == (*(ushort*)(src_p))))
					{
						src_p += 2;
						src_ref += 2;
					}
					if ((src_p < src_LASTLITERALS) && (*src_ref == *src_p)) src_p++;

				_endCount:

					// Encode MatchLength
					length = (int)(src_p - src_anchor);

					if (dst_p + (length >> 8) > dst_LASTLITERALS_1) return 0; // Check output limit

					if (length >= ML_MASK)
					{
						*dst_token += ML_MASK;
						length -= ML_MASK;
						for (; length > 509; length -= 510)
						{
							*dst_p++ = 255;
							*dst_p++ = 255;
						}
						if (length > 254)
						{
							length -= 255;
							*dst_p++ = 255;
						}
						*dst_p++ = (byte)length;
					}
					else
					{
						*dst_token += (byte)length;
					}

					// Test end of chunk
					if (src_p > src_mflimit)
					{
						src_anchor = src_p;
						break;
					}

					// Fill table
					hash_table[((((*(uint*)(src_p - 2))) * 2654435761u) >> HASH_ADJUST)] = (uint)(src_p - 2 - src_base);

					// Test next position

					h = ((((*(uint*)(src_p))) * 2654435761u) >> HASH_ADJUST);
					src_ref = src_base + hash_table[h];
					hash_table[h] = (uint)(src_p - src_base);

					if ((src_ref > src_p - (MAX_DISTANCE + 1)) && ((*(uint*)(src_ref)) == (*(uint*)(src_p))))
					{
						dst_token = dst_p++;
						*dst_token = 0;
						goto _next_match;
					}

					// Prepare next loop
					src_anchor = src_p++;
					h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> HASH_ADJUST);
				}

			_last_literals:

				// Encode Last Literals
				var lastRun = (int)(src_end - src_anchor);
				if (dst_p + lastRun + 1 + ((lastRun + 255 - RUN_MASK) / 255) > dst_end) return 0;
				if (lastRun >= RUN_MASK)
				{
					*dst_p++ = (RUN_MASK << ML_BITS);
					lastRun -= RUN_MASK;
					for (; lastRun > 254; lastRun -= 255) *dst_p++ = 255;
					*dst_p++ = (byte)lastRun;
				}
				else *dst_p++ = (byte)(lastRun << ML_BITS);
				BlockCopy(src_anchor, dst_p, (int)(src_end - src_anchor));
				dst_p += src_end - src_anchor;

				// End
				return (int)(dst_p - dst);
			}
		}

		#endregion

		#region LZ4_compress64kCtx_64

		private static unsafe int LZ4_compress64kCtx_64(
			ushort* hash_table,
			byte* src,
			byte* dst,
			int src_len,
			int dst_maxlen)
		{
			byte* _p;

			fixed (int* debruijn64 = &DEBRUIJN_TABLE_64[0])
			{
				// r93
				var src_p = src;
				var src_anchor = src_p;
				var src_base = src_p;
				var src_end = src_p + src_len;
				var src_mflimit = src_end - MFLIMIT;

				var dst_p = dst;
				var dst_end = dst_p + dst_maxlen;

				var src_LASTLITERALS = src_end - LASTLITERALS;
				var src_LASTLITERALS_1 = src_LASTLITERALS - 1;

				var src_LASTLITERALS_3 = src_LASTLITERALS - 3;

				var src_LASTLITERALS_STEPSIZE_1 = src_LASTLITERALS - (STEPSIZE_64 - 1);
				var dst_LASTLITERALS_1 = dst_end - (1 + LASTLITERALS);
				var dst_LASTLITERALS_3 = dst_end - (2 + 1 + LASTLITERALS);

				int len, length;

				uint h, h_fwd;

				// Init
				if (src_len < MINLENGTH) goto _last_literals;

				// First Byte
				src_p++;
				h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> HASH64K_ADJUST);

				// Main Loop
				while (true)
				{
					var findMatchAttempts = (1 << SKIPSTRENGTH) + 3;
					var src_p_fwd = src_p;
					byte* src_ref;
					byte* dst_token;

					// Find a match
					do
					{
						h = h_fwd;
						var step = findMatchAttempts++ >> SKIPSTRENGTH;
						src_p = src_p_fwd;
						src_p_fwd = src_p + step;

						if (src_p_fwd > src_mflimit) goto _last_literals;

						h_fwd = ((((*(uint*)(src_p_fwd))) * 2654435761u) >> HASH64K_ADJUST);
						src_ref = src_base + hash_table[h];
						hash_table[h] = (ushort)(src_p - src_base);
					} while ((*(uint*)(src_ref)) != (*(uint*)(src_p)));

					// Catch up
					while ((src_p > src_anchor) && (src_ref > src) && (src_p[-1] == src_ref[-1]))
					{
						src_p--;
						src_ref--;
					}

					// Encode Literal length
					length = (int)(src_p - src_anchor);
					dst_token = dst_p++;

					if (dst_p + length + (length >> 8) > dst_LASTLITERALS_3) return 0; // Check output limit

					if (length >= RUN_MASK)
					{
						len = length - RUN_MASK;
						*dst_token = (RUN_MASK << ML_BITS);
						if (len > 254)
						{
							do
							{
								*dst_p++ = 255;
								len -= 255;
							} while (len > 254);
							*dst_p++ = (byte)len;
							BlockCopy(src_anchor, dst_p, (length));
							dst_p += length;
							goto _next_match;
						}
						*dst_p++ = (byte)len;
					}
					else
					{
						*dst_token = (byte)(length << ML_BITS);
					}

					// Copy Literals
					{
						_p = dst_p + (length);
						{
							do
							{
								*(ulong*)dst_p = *(ulong*)src_anchor;
								dst_p += 8;
								src_anchor += 8;
							} while (dst_p < _p);
						}
						dst_p = _p;
					}

				_next_match:

					// Encode Offset
					*(ushort*)dst_p = (ushort)(src_p - src_ref);
					dst_p += 2;

					// Start Counting
					src_p += MINMATCH;
					src_ref += MINMATCH; // MinMatch verified
					src_anchor = src_p;

					while (src_p < src_LASTLITERALS_STEPSIZE_1)
					{
						var diff = (*(long*)(src_ref)) ^ (*(long*)(src_p));
						if (diff == 0)
						{
							src_p += STEPSIZE_64;
							src_ref += STEPSIZE_64;
							continue;
						}
						src_p += debruijn64[(((ulong)((diff) & -(diff)) * 0x0218A392CDABBD3FL)) >> 58];
						goto _endCount;
					}

					if ((src_p < src_LASTLITERALS_3) && ((*(uint*)(src_ref)) == (*(uint*)(src_p))))
					{
						src_p += 4;
						src_ref += 4;
					}
					if ((src_p < src_LASTLITERALS_1) && ((*(ushort*)(src_ref)) == (*(ushort*)(src_p))))
					{
						src_p += 2;
						src_ref += 2;
					}
					if ((src_p < src_LASTLITERALS) && (*src_ref == *src_p)) src_p++;

				_endCount:

					// Encode MatchLength
					len = (int)(src_p - src_anchor);

					if (dst_p + (len >> 8) > dst_LASTLITERALS_1) return 0; // Check output limit

					if (len >= ML_MASK)
					{
						*dst_token += ML_MASK;
						len -= ML_MASK;
						for (; len > 509; len -= 510)
						{
							*dst_p++ = 255;
							*dst_p++ = 255;
						}
						if (len > 254)
						{
							len -= 255;
							*dst_p++ = 255;
						}
						*dst_p++ = (byte)len;
					}
					else
					{
						*dst_token += (byte)len;
					}

					// Test end of chunk
					if (src_p > src_mflimit)
					{
						src_anchor = src_p;
						break;
					}

					// Fill table
					hash_table[((((*(uint*)(src_p - 2))) * 2654435761u) >> HASH64K_ADJUST)] = (ushort)(src_p - 2 - src_base);

					// Test next position

					h = ((((*(uint*)(src_p))) * 2654435761u) >> HASH64K_ADJUST);
					src_ref = src_base + hash_table[h];
					hash_table[h] = (ushort)(src_p - src_base);

					if ((*(uint*)(src_ref)) == (*(uint*)(src_p)))
					{
						dst_token = dst_p++;
						*dst_token = 0;
						goto _next_match;
					}

					// Prepare next loop
					src_anchor = src_p++;
					h_fwd = ((((*(uint*)(src_p))) * 2654435761u) >> HASH64K_ADJUST);
				}

			_last_literals:

				// Encode Last Literals
				var lastRun = (int)(src_end - src_anchor);
				if (dst_p + lastRun + 1 + (lastRun - RUN_MASK + 255) / 255 > dst_end) return 0;
				if (lastRun >= RUN_MASK)
				{
					*dst_p++ = (RUN_MASK << ML_BITS);
					lastRun -= RUN_MASK;
					for (; lastRun > 254; lastRun -= 255) *dst_p++ = 255;
					*dst_p++ = (byte)lastRun;
				}
				else *dst_p++ = (byte)(lastRun << ML_BITS);
				BlockCopy(src_anchor, dst_p, (int)(src_end - src_anchor));
				dst_p += src_end - src_anchor;

				// End
				return (int)(dst_p - dst);
			}
		}

		private unsafe static void BlockCopy(byte* src, byte* dest, int len)
		{
			NativeMethods.memcpy(dest, src, len);
		}

		#endregion

		#region LZ4_uncompress_64

		private static unsafe int LZ4_uncompress_64(
			byte* src,
			byte* dst,
			int dst_len)
		{
			fixed (int* dec32table = &DECODER_TABLE_32[0])
			fixed (int* dec64table = &DECODER_TABLE_64[0])
			{
				// r93
				var src_p = src;
				byte* dst_ref;

				var dst_p = dst;
				var dst_end = dst_p + dst_len;
				byte* dst_cpy;

				var dst_LASTLITERALS = dst_end - LASTLITERALS;
				var dst_COPYLENGTH = dst_end - COPYLENGTH;
				var dst_COPYLENGTH_STEPSIZE_4 = dst_end - COPYLENGTH - (STEPSIZE_64 - 4);

				byte token;

				// Main Loop
				while (true)
				{
					int length;

					// get runlength
					token = *src_p++;
					if ((length = (token >> ML_BITS)) == RUN_MASK)
					{
						int len;
						for (; (len = *src_p++) == 255; length += 255)
						{
							/* do nothing */
						}
						length += len;
					}

					// copy literals
					dst_cpy = dst_p + length;

					if (dst_cpy > dst_COPYLENGTH)
					{
						if (dst_cpy != dst_end) goto _output_error; // Error : not enough place for another match (min 4) + 5 literals
						BlockCopy(src_p, dst_p, (length));
						src_p += length;
						break; // EOF
					}
					do
					{
						*(ulong*)dst_p = *(ulong*)src_p;
						dst_p += 8;
						src_p += 8;
					} while (dst_p < dst_cpy);
					src_p -= (dst_p - dst_cpy);
					dst_p = dst_cpy;

					// get offset
					dst_ref = (dst_cpy) - (*(ushort*)(src_p));
					src_p += 2;
					if (dst_ref < dst) goto _output_error; // Error : offset outside destination buffer

					// get matchlength
					if ((length = (token & ML_MASK)) == ML_MASK)
					{
						for (; *src_p == 255; length += 255) src_p++;
						length += *src_p++;
					}

					// copy repeated sequence
					if ((dst_p - dst_ref) < STEPSIZE_64)
					{
						var dec64 = dec64table[dst_p - dst_ref];

						dst_p[0] = dst_ref[0];
						dst_p[1] = dst_ref[1];
						dst_p[2] = dst_ref[2];
						dst_p[3] = dst_ref[3];
						dst_p += 4;
						dst_ref += 4;
						dst_ref -= dec32table[dst_p - dst_ref];
						(*(uint*)(dst_p)) = (*(uint*)(dst_ref));
						dst_p += STEPSIZE_64 - 4;
						dst_ref -= dec64;
					}
					else
					{
						*(ulong*)dst_p = *(ulong*)dst_ref;
						dst_p += 8;
						dst_ref += 8;
					}
					dst_cpy = dst_p + length - (STEPSIZE_64 - 4);

					if (dst_cpy > dst_COPYLENGTH_STEPSIZE_4)
					{
						if (dst_cpy > dst_LASTLITERALS) goto _output_error; // Error : last 5 bytes must be literals
						while (dst_p < dst_COPYLENGTH)
						{
							*(ulong*)dst_p = *(ulong*)dst_ref;
							dst_p += 8;
							dst_ref += 8;
						}

						while (dst_p < dst_cpy) *dst_p++ = *dst_ref++;
						dst_p = dst_cpy;
						continue;
					}

					{
						do
						{
							*(ulong*)dst_p = *(ulong*)dst_ref;
							dst_p += 8;
							dst_ref += 8;
						} while (dst_p < dst_cpy);
					}
					dst_p = dst_cpy; // correction
				}

				// end of decoding
				return (int)((src_p) - src);

				// write overflow error detected
			_output_error:
				return (int)(-((src_p) - src));
			}
		}

		#endregion

		#region LZ4_uncompress_unknownOutputSize_64

		private static unsafe int LZ4_uncompress_unknownOutputSize_64(
			byte* src,
			byte* dst,
			int src_len,
			int dst_maxlen)
		{
			fixed (int* dec32table = &DECODER_TABLE_32[0])
			fixed (int* dec64table = &DECODER_TABLE_64[0])
			{
				// r93
				var src_p = src;
				var src_end = src_p + src_len;
				byte* dst_ref;

				var dst_p = dst;
				var dst_end = dst_p + dst_maxlen;
				byte* dst_cpy;

				var src_LASTLITERALS_3 = (src_end - (2 + 1 + LASTLITERALS));
				var src_LASTLITERALS_1 = (src_end - (LASTLITERALS + 1));
				var dst_COPYLENGTH = (dst_end - COPYLENGTH);
				var dst_COPYLENGTH_STEPSIZE_4 = (dst_end - (COPYLENGTH + (STEPSIZE_64 - 4)));
				var dst_LASTLITERALS = (dst_end - LASTLITERALS);
				var dst_MFLIMIT = (dst_end - MFLIMIT);

				// Special case
				if (src_p == src_end) goto _output_error; // A correctly formed null-compressed LZ4 must have at least one byte (token=0)

				// Main Loop
				while (true)
				{
					byte token;
					int length;

					// get runlength
					token = *src_p++;
					if ((length = (token >> ML_BITS)) == RUN_MASK)
					{
						var s = 255;
						while ((src_p < src_end) && (s == 255))
						{
							s = *src_p++;
							length += s;
						}
					}

					// copy literals
					dst_cpy = dst_p + length;

					if ((dst_cpy > dst_MFLIMIT) || (src_p + length > src_LASTLITERALS_3))
					{
						if (dst_cpy > dst_end) goto _output_error; // Error : writes beyond output buffer
						if (src_p + length != src_end) goto _output_error; // Error : LZ4 format requires to consume all input at this stage (no match within the last 11 bytes, and at least 8 remaining input bytes for another match+literals)
						BlockCopy(src_p, dst_p, (length));
						dst_p += length;
						break; // Necessarily EOF, due to parsing restrictions
					}
					do
					{
						*(ulong*)dst_p = *(ulong*)src_p;
						dst_p += 8;
						src_p += 8;
					} while (dst_p < dst_cpy);
					src_p -= (dst_p - dst_cpy);
					dst_p = dst_cpy;

					// get offset
					dst_ref = (dst_cpy) - (*(ushort*)(src_p));
					src_p += 2;
					if (dst_ref < dst) goto _output_error; // Error : offset outside of destination buffer

					// get matchlength
					if ((length = (token & ML_MASK)) == ML_MASK)
					{
						while (src_p < src_LASTLITERALS_1) // Error : a minimum input bytes must remain for LASTLITERALS + token
						{
							int s = *src_p++;
							length += s;
							if (s == 255) continue;
							break;
						}
					}

					// copy repeated sequence
					if (dst_p - dst_ref < STEPSIZE_64)
					{
						var dec64 = dec64table[dst_p - dst_ref];

						dst_p[0] = dst_ref[0];
						dst_p[1] = dst_ref[1];
						dst_p[2] = dst_ref[2];
						dst_p[3] = dst_ref[3];
						dst_p += 4;
						dst_ref += 4;
						dst_ref -= dec32table[dst_p - dst_ref];
						(*(uint*)(dst_p)) = (*(uint*)(dst_ref));
						dst_p += STEPSIZE_64 - 4;
						dst_ref -= dec64;
					}
					else
					{
						*(ulong*)dst_p = *(ulong*)dst_ref;
						dst_p += 8;
						dst_ref += 8;
					}
					dst_cpy = dst_p + length - (STEPSIZE_64 - 4);

					if (dst_cpy > dst_COPYLENGTH_STEPSIZE_4)
					{
						if (dst_cpy > dst_LASTLITERALS) goto _output_error; // Error : last 5 bytes must be literals
						while (dst_p < dst_COPYLENGTH)
						{
							*(ulong*)dst_p = *(ulong*)dst_ref;
							dst_p += 8;
							dst_ref += 8;
						}

						while (dst_p < dst_cpy) *dst_p++ = *dst_ref++;
						dst_p = dst_cpy;
						continue;
					}

					do
					{
						*(ulong*)dst_p = *(ulong*)dst_ref;
						dst_p += 8;
						dst_ref += 8;
					} while (dst_p < dst_cpy);
					dst_p = dst_cpy; // correction
				}

				// end of decoding
				return (int)(dst_p - dst);

			_output_error:

				// write overflow error detected
				return (int)-(src_p - src);
			}
		}

		#endregion

		public void Dispose()
		{
			Marshal.FreeHGlobal(new IntPtr(_hashtable64K));
			Marshal.FreeHGlobal(new IntPtr(_hashtable));
		}
	}
}

// ReSharper restore JoinDeclarationAndInitializer
// ReSharper restore TooWideLocalVariableScope
// ReSharper restore InconsistentNaming