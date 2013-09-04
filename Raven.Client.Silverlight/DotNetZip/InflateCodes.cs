using System;

namespace Ionic.Zlib
{
	sealed class InflateCodes
	{
		// waiting for "i:"=input,
		//             "o:"=output,
		//             "x:"=nothing
		private const int START   = 0; // x: set up for LEN
		private const int LEN     = 1; // i: get length/literal/eob next
		private const int LENEXT  = 2; // i: getting length extra (have base)
		private const int DIST    = 3; // i: get distance next
		private const int DISTEXT = 4; // i: getting distance extra
		private const int COPY    = 5; // o: copying bytes in window, waiting for space
		private const int LIT     = 6; // o: got literal, waiting for output space
		private const int WASH    = 7; // o: got eob, possibly still output waiting
		private const int END     = 8; // x: got eob and all data flushed
		private const int BADCODE = 9; // x: got error

		internal int mode;        // current inflate_codes mode

		// mode dependent information
		internal int len;

		internal int[] tree;      // pointer into tree
		internal int tree_index = 0;
		internal int need;        // bits needed

		internal int lit;

		// if EXT or COPY, where and how much
		internal int bitsToGet;   // bits to get for extra
		internal int dist;        // distance back to copy from

		internal byte lbits;      // ltree bits decoded per branch
		internal byte dbits;      // dtree bits decoder per branch
		internal int[] ltree;     // literal/length/eob tree
		internal int ltree_index; // literal/length/eob tree
		internal int[] dtree;     // distance tree
		internal int dtree_index; // distance tree

		internal InflateCodes()
		{
		}

		internal void Init(int bl, int bd, int[] tl, int tl_index, int[] td, int td_index)
		{
			mode = START;
			lbits = (byte)bl;
			dbits = (byte)bd;
			ltree = tl;
			ltree_index = tl_index;
			dtree = td;
			dtree_index = td_index;
			tree = null;
		}

		internal int Process(InflateBlocks blocks, int r)
		{
			int j;      // temporary storage
			int tindex; // temporary pointer
			int e;      // extra bits or operation
			int b = 0;  // bit buffer
			int k = 0;  // bits in bit buffer
			int p = 0;  // input data pointer
			int n;      // bytes available there
			int q;      // output window write pointer
			int m;      // bytes to end of window or read pointer
			int f;      // pointer to copy strings from

			ZlibCodec z = blocks._codec;

			// copy input/output information to locals (UPDATE macro restores)
			p = z.NextIn;
			n = z.AvailableBytesIn;
			b = blocks.bitb;
			k = blocks.bitk;
			q = blocks.writeAt; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;

			// process input and output based on current state
			while (true)
			{
				switch (mode)
				{
						// waiting for "i:"=input, "o:"=output, "x:"=nothing
					case START:  // x: set up for LEN
						if (m >= 258 && n >= 10)
						{
							blocks.bitb = b; blocks.bitk = k;
							z.AvailableBytesIn = n;
							z.TotalBytesIn += p - z.NextIn;
							z.NextIn = p;
							blocks.writeAt = q;
							r = InflateFast(lbits, dbits, ltree, ltree_index, dtree, dtree_index, blocks, z);

							p = z.NextIn;
							n = z.AvailableBytesIn;
							b = blocks.bitb;
							k = blocks.bitk;
							q = blocks.writeAt; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;

							if (r != ZlibConstants.Z_OK)
							{
								mode = (r == ZlibConstants.Z_STREAM_END) ? WASH : BADCODE;
								break;
							}
						}
						need = lbits;
						tree = ltree;
						tree_index = ltree_index;

						mode = LEN;
						goto case LEN;

					case LEN:  // i: get length/literal/eob next
						j = need;

						while (k < j)
						{
							if (n != 0)
								r = ZlibConstants.Z_OK;
							else
							{
								blocks.bitb = b; blocks.bitk = k;
								z.AvailableBytesIn = n;
								z.TotalBytesIn += p - z.NextIn;
								z.NextIn = p;
								blocks.writeAt = q;
								return blocks.Flush(r);
							}
							n--;
							b |= (z.InputBuffer[p++] & 0xff) << k;
							k += 8;
						}

						tindex = (tree_index + (b & InternalInflateConstants.InflateMask[j])) * 3;

						b >>= (tree[tindex + 1]);
						k -= (tree[tindex + 1]);

						e = tree[tindex];

						if (e == 0)
						{
							// literal
							lit = tree[tindex + 2];
							mode = LIT;
							break;
						}
						if ((e & 16) != 0)
						{
							// length
							bitsToGet = e & 15;
							len = tree[tindex + 2];
							mode = LENEXT;
							break;
						}
						if ((e & 64) == 0)
						{
							// next table
							need = e;
							tree_index = tindex / 3 + tree[tindex + 2];
							break;
						}
						if ((e & 32) != 0)
						{
							// end of block
							mode = WASH;
							break;
						}
						mode = BADCODE; // invalid code
						z.Message = "invalid literal/length code";
						r = ZlibConstants.Z_DATA_ERROR;

						blocks.bitb = b; blocks.bitk = k;
						z.AvailableBytesIn = n;
						z.TotalBytesIn += p - z.NextIn;
						z.NextIn = p;
						blocks.writeAt = q;
						return blocks.Flush(r);


					case LENEXT:  // i: getting length extra (have base)
						j = bitsToGet;

						while (k < j)
						{
							if (n != 0)
								r = ZlibConstants.Z_OK;
							else
							{
								blocks.bitb = b; blocks.bitk = k;
								z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
								blocks.writeAt = q;
								return blocks.Flush(r);
							}
							n--; b |= (z.InputBuffer[p++] & 0xff) << k;
							k += 8;
						}

						len += (b & InternalInflateConstants.InflateMask[j]);

						b >>= j;
						k -= j;

						need = dbits;
						tree = dtree;
						tree_index = dtree_index;
						mode = DIST;
						goto case DIST;

					case DIST:  // i: get distance next
						j = need;

						while (k < j)
						{
							if (n != 0)
								r = ZlibConstants.Z_OK;
							else
							{
								blocks.bitb = b; blocks.bitk = k;
								z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
								blocks.writeAt = q;
								return blocks.Flush(r);
							}
							n--; b |= (z.InputBuffer[p++] & 0xff) << k;
							k += 8;
						}

						tindex = (tree_index + (b & InternalInflateConstants.InflateMask[j])) * 3;

						b >>= tree[tindex + 1];
						k -= tree[tindex + 1];

						e = (tree[tindex]);
						if ((e & 0x10) != 0)
						{
							// distance
							bitsToGet = e & 15;
							dist = tree[tindex + 2];
							mode = DISTEXT;
							break;
						}
						if ((e & 64) == 0)
						{
							// next table
							need = e;
							tree_index = tindex / 3 + tree[tindex + 2];
							break;
						}
						mode = BADCODE; // invalid code
						z.Message = "invalid distance code";
						r = ZlibConstants.Z_DATA_ERROR;

						blocks.bitb = b; blocks.bitk = k;
						z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
						blocks.writeAt = q;
						return blocks.Flush(r);


					case DISTEXT:  // i: getting distance extra
						j = bitsToGet;

						while (k < j)
						{
							if (n != 0)
								r = ZlibConstants.Z_OK;
							else
							{
								blocks.bitb = b; blocks.bitk = k;
								z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
								blocks.writeAt = q;
								return blocks.Flush(r);
							}
							n--; b |= (z.InputBuffer[p++] & 0xff) << k;
							k += 8;
						}

						dist += (b & InternalInflateConstants.InflateMask[j]);

						b >>= j;
						k -= j;

						mode = COPY;
						goto case COPY;

					case COPY:  // o: copying bytes in window, waiting for space
						f = q - dist;
						while (f < 0)
						{
							// modulo window size-"while" instead
							f += blocks.end; // of "if" handles invalid distances
						}
						while (len != 0)
						{
							if (m == 0)
							{
								if (q == blocks.end && blocks.readAt != 0)
								{
									q = 0; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;
								}
								if (m == 0)
								{
									blocks.writeAt = q; r = blocks.Flush(r);
									q = blocks.writeAt; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;

									if (q == blocks.end && blocks.readAt != 0)
									{
										q = 0; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;
									}

									if (m == 0)
									{
										blocks.bitb = b; blocks.bitk = k;
										z.AvailableBytesIn = n;
										z.TotalBytesIn += p - z.NextIn;
										z.NextIn = p;
										blocks.writeAt = q;
										return blocks.Flush(r);
									}
								}
							}

							blocks.window[q++] = blocks.window[f++]; m--;

							if (f == blocks.end)
								f = 0;
							len--;
						}
						mode = START;
						break;

					case LIT:  // o: got literal, waiting for output space
						if (m == 0)
						{
							if (q == blocks.end && blocks.readAt != 0)
							{
								q = 0; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;
							}
							if (m == 0)
							{
								blocks.writeAt = q; r = blocks.Flush(r);
								q = blocks.writeAt; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;

								if (q == blocks.end && blocks.readAt != 0)
								{
									q = 0; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;
								}
								if (m == 0)
								{
									blocks.bitb = b; blocks.bitk = k;
									z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
									blocks.writeAt = q;
									return blocks.Flush(r);
								}
							}
						}
						r = ZlibConstants.Z_OK;

						blocks.window[q++] = (byte)lit; m--;

						mode = START;
						break;

					case WASH:  // o: got eob, possibly more output
						if (k > 7)
						{
							// return unused byte, if any
							k -= 8;
							n++;
							p--; // can always return one
						}

						blocks.writeAt = q; r = blocks.Flush(r);
						q = blocks.writeAt; m = q < blocks.readAt ? blocks.readAt - q - 1 : blocks.end - q;

						if (blocks.readAt != blocks.writeAt)
						{
							blocks.bitb = b; blocks.bitk = k;
							z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
							blocks.writeAt = q;
							return blocks.Flush(r);
						}
						mode = END;
						goto case END;

					case END:
						r = ZlibConstants.Z_STREAM_END;
						blocks.bitb = b; blocks.bitk = k;
						z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
						blocks.writeAt = q;
						return blocks.Flush(r);

					case BADCODE:  // x: got error

						r = ZlibConstants.Z_DATA_ERROR;

						blocks.bitb = b; blocks.bitk = k;
						z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
						blocks.writeAt = q;
						return blocks.Flush(r);

					default:
						r = ZlibConstants.Z_STREAM_ERROR;

						blocks.bitb = b; blocks.bitk = k;
						z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
						blocks.writeAt = q;
						return blocks.Flush(r);
				}
			}
		}


		// Called with number of bytes left to write in window at least 258
		// (the maximum string length) and number of input bytes available
		// at least ten.  The ten bytes are six bytes for the longest length/
		// distance pair plus four bytes for overloading the bit buffer.

		internal int InflateFast(int bl, int bd, int[] tl, int tl_index, int[] td, int td_index, InflateBlocks s, ZlibCodec z)
		{
			int t;        // temporary pointer
			int[] tp;     // temporary pointer
			int tp_index; // temporary pointer
			int e;        // extra bits or operation
			int b;        // bit buffer
			int k;        // bits in bit buffer
			int p;        // input data pointer
			int n;        // bytes available there
			int q;        // output window write pointer
			int m;        // bytes to end of window or read pointer
			int ml;       // mask for literal/length tree
			int md;       // mask for distance tree
			int c;        // bytes to copy
			int d;        // distance back to copy from
			int r;        // copy source pointer

			int tp_index_t_3; // (tp_index+t)*3

			// load input, output, bit values
			p = z.NextIn; n = z.AvailableBytesIn; b = s.bitb; k = s.bitk;
			q = s.writeAt; m = q < s.readAt ? s.readAt - q - 1 : s.end - q;

			// initialize masks
			ml = InternalInflateConstants.InflateMask[bl];
			md = InternalInflateConstants.InflateMask[bd];

			// do until not enough input or output space for fast loop
			do
			{
				// assume called with m >= 258 && n >= 10
				// get literal/length code
				while (k < (20))
				{
					// max bits for literal/length code
					n--;
					b |= (z.InputBuffer[p++] & 0xff) << k; k += 8;
				}

				t = b & ml;
				tp = tl;
				tp_index = tl_index;
				tp_index_t_3 = (tp_index + t) * 3;
				if ((e = tp[tp_index_t_3]) == 0)
				{
					b >>= (tp[tp_index_t_3 + 1]); k -= (tp[tp_index_t_3 + 1]);

					s.window[q++] = (byte)tp[tp_index_t_3 + 2];
					m--;
					continue;
				}
				do
				{

					b >>= (tp[tp_index_t_3 + 1]); k -= (tp[tp_index_t_3 + 1]);

					if ((e & 16) != 0)
					{
						e &= 15;
						c = tp[tp_index_t_3 + 2] + ((int)b & InternalInflateConstants.InflateMask[e]);

						b >>= e; k -= e;

						// decode distance base of block to copy
						while (k < 15)
						{
							// max bits for distance code
							n--;
							b |= (z.InputBuffer[p++] & 0xff) << k; k += 8;
						}

						t = b & md;
						tp = td;
						tp_index = td_index;
						tp_index_t_3 = (tp_index + t) * 3;
						e = tp[tp_index_t_3];

						do
						{

							b >>= (tp[tp_index_t_3 + 1]); k -= (tp[tp_index_t_3 + 1]);

							if ((e & 16) != 0)
							{
								// get extra bits to add to distance base
								e &= 15;
								while (k < e)
								{
									// get extra bits (up to 13)
									n--;
									b |= (z.InputBuffer[p++] & 0xff) << k; k += 8;
								}

								d = tp[tp_index_t_3 + 2] + (b & InternalInflateConstants.InflateMask[e]);

								b >>= e; k -= e;

								// do the copy
								m -= c;
								if (q >= d)
								{
									// offset before dest
									//  just copy
									r = q - d;
									if (q - r > 0 && 2 > (q - r))
									{
										s.window[q++] = s.window[r++]; // minimum count is three,
										s.window[q++] = s.window[r++]; // so unroll loop a little
										c -= 2;
									}
									else
									{
										Array.Copy(s.window, r, s.window, q, 2);
										q += 2; r += 2; c -= 2;
									}
								}
								else
								{
									// else offset after destination
									r = q - d;
									do
									{
										r += s.end; // force pointer in window
									}
									while (r < 0); // covers invalid distances
									e = s.end - r;
									if (c > e)
									{
										// if source crosses,
										c -= e; // wrapped copy
										if (q - r > 0 && e > (q - r))
										{
											do
											{
												s.window[q++] = s.window[r++];
											}
											while (--e != 0);
										}
										else
										{
											Array.Copy(s.window, r, s.window, q, e);
											q += e; r += e; e = 0;
										}
										r = 0; // copy rest from start of window
									}
								}

								// copy all or what's left
								if (q - r > 0 && c > (q - r))
								{
									do
									{
										s.window[q++] = s.window[r++];
									}
									while (--c != 0);
								}
								else
								{
									Array.Copy(s.window, r, s.window, q, c);
									q += c; r += c; c = 0;
								}
								break;
							}
							else if ((e & 64) == 0)
							{
								t += tp[tp_index_t_3 + 2];
								t += (b & InternalInflateConstants.InflateMask[e]);
								tp_index_t_3 = (tp_index + t) * 3;
								e = tp[tp_index_t_3];
							}
							else
							{
								z.Message = "invalid distance code";

								c = z.AvailableBytesIn - n; c = (k >> 3) < c ? k >> 3 : c; n += c; p -= c; k -= (c << 3);

								s.bitb = b; s.bitk = k;
								z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
								s.writeAt = q;

								return ZlibConstants.Z_DATA_ERROR;
							}
						}
						while (true);
						break;
					}

					if ((e & 64) == 0)
					{
						t += tp[tp_index_t_3 + 2];
						t += (b & InternalInflateConstants.InflateMask[e]);
						tp_index_t_3 = (tp_index + t) * 3;
						if ((e = tp[tp_index_t_3]) == 0)
						{
							b >>= (tp[tp_index_t_3 + 1]); k -= (tp[tp_index_t_3 + 1]);
							s.window[q++] = (byte)tp[tp_index_t_3 + 2];
							m--;
							break;
						}
					}
					else if ((e & 32) != 0)
					{
						c = z.AvailableBytesIn - n; c = (k >> 3) < c ? k >> 3 : c; n += c; p -= c; k -= (c << 3);

						s.bitb = b; s.bitk = k;
						z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
						s.writeAt = q;

						return ZlibConstants.Z_STREAM_END;
					}
					else
					{
						z.Message = "invalid literal/length code";

						c = z.AvailableBytesIn - n; c = (k >> 3) < c ? k >> 3 : c; n += c; p -= c; k -= (c << 3);

						s.bitb = b; s.bitk = k;
						z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
						s.writeAt = q;

						return ZlibConstants.Z_DATA_ERROR;
					}
				}
				while (true);
			}
			while (m >= 258 && n >= 10);

			// not enough input or output--restore pointers and return
			c = z.AvailableBytesIn - n; c = (k >> 3) < c ? k >> 3 : c; n += c; p -= c; k -= (c << 3);

			s.bitb = b; s.bitk = k;
			z.AvailableBytesIn = n; z.TotalBytesIn += p - z.NextIn; z.NextIn = p;
			s.writeAt = q;

			return ZlibConstants.Z_OK;
		}
	}
}