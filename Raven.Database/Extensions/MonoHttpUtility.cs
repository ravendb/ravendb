//-----------------------------------------------------------------------
// <copyright file="MonoHttpUtility.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Raven.Database.Extensions
{
	/// <summary>
	/// We have to use this stupid trick because HttpUtility.UrlEncode / Decode
	/// uses HttpContext.Current under the covers, which doesn't work in IIS7 
	/// Application_Start
	/// </summary>
	public class MonoHttpUtility
	{
		public static string UrlDecode(string str)
		{
			return UrlDecode(str, Encoding.UTF8);
		}

		static char[] GetChars(MemoryStream b, Encoding e)
		{
			return e.GetChars(b.GetBuffer(), 0, (int)b.Length);
		}

		static void WriteCharBytes(IList buf, char ch, Encoding e)
		{
			if (ch > 255)
			{
				foreach (byte b in e.GetBytes(new char[] { ch }))
					buf.Add(b);
			}
			else
				buf.Add((byte)ch);
		}

		public static string UrlDecode(string s, Encoding e)
		{
			if (null == s)
				return null;

			if (s.IndexOf('%') == -1 && s.IndexOf('+') == -1)
				return s;

			if (e == null)
				e = Encoding.UTF8;

			long len = s.Length;
			var bytes = new List<byte>();
			int xchar;
			char ch;

			for (int i = 0; i < len; i++)
			{
				ch = s[i];
				if (ch == '%' && i + 2 < len && s[i + 1] != '%')
				{
					if (s[i + 1] == 'u' && i + 5 < len)
					{
						// Unicode hex sequence
						xchar = GetChar(s, i + 2, 4);
						if (xchar != -1)
						{
							WriteCharBytes(bytes, (char)xchar, e);
							i += 5;
						}
						else
							WriteCharBytes(bytes, '%', e);
					}
					else if ((xchar = GetChar(s, i + 1, 2)) != -1)
					{
						WriteCharBytes(bytes, (char)xchar, e);
						i += 2;
					}
					else
					{
						WriteCharBytes(bytes, '%', e);
					}
					continue;
				}

				if (ch == '+')
					WriteCharBytes(bytes, ' ', e);
				else
					WriteCharBytes(bytes, ch, e);
			}

			byte[] buf = bytes.ToArray();
			bytes = null;
			return e.GetString(buf);

		}

		public static string UrlDecode(byte[] bytes, Encoding e)
		{
			if (bytes == null)
				return null;

			return UrlDecode(bytes, 0, bytes.Length, e);
		}

		static int GetInt(byte b)
		{
			char c = (char)b;
			if (c >= '0' && c <= '9')
				return c - '0';

			if (c >= 'a' && c <= 'f')
				return c - 'a' + 10;

			if (c >= 'A' && c <= 'F')
				return c - 'A' + 10;

			return -1;
		}

		static int GetChar(byte[] bytes, int offset, int length)
		{
			int value = 0;
			int end = length + offset;
			for (int i = offset; i < end; i++)
			{
				int current = GetInt(bytes[i]);
				if (current == -1)
					return -1;
				value = (value << 4) + current;
			}

			return value;
		}

		static int GetChar(string str, int offset, int length)
		{
			int val = 0;
			int end = length + offset;
			for (int i = offset; i < end; i++)
			{
				char c = str[i];
				if (c > 127)
					return -1;

				int current = GetInt((byte)c);
				if (current == -1)
					return -1;
				val = (val << 4) + current;
			}

			return val;
		}

		public static string UrlDecode(byte[] bytes, int offset, int count, Encoding e)
		{
			if (bytes == null)
				return null;
			if (count == 0)
				return String.Empty;

			if (bytes == null)
				throw new ArgumentNullException("bytes");

			if (offset < 0 || offset > bytes.Length)
				throw new ArgumentOutOfRangeException("offset");

			if (count < 0 || offset + count > bytes.Length)
				throw new ArgumentOutOfRangeException("count");

			StringBuilder output = new StringBuilder();
			MemoryStream acc = new MemoryStream();

			int end = count + offset;
			int xchar;
			for (int i = offset; i < end; i++)
			{
				if (bytes[i] == '%' && i + 2 < count && bytes[i + 1] != '%')
				{
					if (bytes[i + 1] == (byte)'u' && i + 5 < end)
					{
						if (acc.Length > 0)
						{
							output.Append(GetChars(acc, e));
							acc.SetLength(0);
						}
						xchar = GetChar(bytes, i + 2, 4);
						if (xchar != -1)
						{
							output.Append((char)xchar);
							i += 5;
							continue;
						}
					}
					else if ((xchar = GetChar(bytes, i + 1, 2)) != -1)
					{
						acc.WriteByte((byte)xchar);
						i += 2;
						continue;
					}
				}

				if (acc.Length > 0)
				{
					output.Append(GetChars(acc, e));
					acc.SetLength(0);
				}

				if (bytes[i] == '+')
				{
					output.Append(' ');
				}
				else
				{
					output.Append((char)bytes[i]);
				}
			}

			if (acc.Length > 0)
			{
				output.Append(GetChars(acc, e));
			}

			acc = null;
			return output.ToString();
		}

		public static byte[] UrlDecodeToBytes(byte[] bytes)
		{
			if (bytes == null)
				return null;

			return UrlDecodeToBytes(bytes, 0, bytes.Length);
		}

		public static byte[] UrlDecodeToBytes(string str)
		{
			return UrlDecodeToBytes(str, Encoding.UTF8);
		}

		public static byte[] UrlDecodeToBytes(string str, Encoding e)
		{
			if (str == null)
				return null;

			if (e == null)
				throw new ArgumentNullException("e");

			return UrlDecodeToBytes(e.GetBytes(str));
		}

		public static byte[] UrlDecodeToBytes(byte[] bytes, int offset, int count)
		{
			if (bytes == null)
				return null;
			if (count == 0)
				return new byte[0];

			int len = bytes.Length;
			if (offset < 0 || offset >= len)
				throw new ArgumentOutOfRangeException("offset");

			if (count < 0 || offset > len - count)
				throw new ArgumentOutOfRangeException("count");

			MemoryStream result = new MemoryStream();
			int end = offset + count;
			for (int i = offset; i < end; i++)
			{
				char c = (char)bytes[i];
				if (c == '+')
				{
					c = ' ';
				}
				else if (c == '%' && i < end - 2)
				{
					int xchar = GetChar(bytes, i + 1, 2);
					if (xchar != -1)
					{
						c = (char)xchar;
						i += 2;
					}
				}
				result.WriteByte((byte)c);
			}

			return result.ToArray();
		}

		public static string UrlEncode(string str)
		{
			return UrlEncode(str, Encoding.UTF8);
		}

		public static string UrlEncode(string s, Encoding Enc)
		{
			if (s == null)
				return null;

			if (s == String.Empty)
				return String.Empty;

			bool needEncode = false;
			int len = s.Length;
			for (int i = 0; i < len; i++)
			{
				char c = s[i];
				if ((c < '0') || (c < 'A' && c > '9') || (c > 'Z' && c < 'a') || (c > 'z'))
				{
					if (MonoHttpEncoder.NotEncoded(c))
						continue;

					needEncode = true;
					break;
				}
			}

			if (!needEncode)
				return s;

			// avoided GetByteCount call
			byte[] bytes = new byte[Enc.GetMaxByteCount(s.Length)];
			int realLen = Enc.GetBytes(s, 0, s.Length, bytes, 0);
			return Encoding.ASCII.GetString(UrlEncodeToBytes(bytes, 0, realLen));
		}

		public static string UrlEncode(byte[] bytes)
		{
			if (bytes == null)
				return null;

			if (bytes.Length == 0)
				return String.Empty;

			return Encoding.ASCII.GetString(UrlEncodeToBytes(bytes, 0, bytes.Length));
		}

		public static string UrlEncode(byte[] bytes, int offset, int count)
		{
			if (bytes == null)
				return null;

			if (bytes.Length == 0)
				return String.Empty;

			return Encoding.ASCII.GetString(UrlEncodeToBytes(bytes, offset, count));
		}

		public static byte[] UrlEncodeToBytes(string str)
		{
			return UrlEncodeToBytes(str, Encoding.UTF8);
		}

		public static byte[] UrlEncodeToBytes(string str, Encoding e)
		{
			if (str == null)
				return null;

			if (str.Length == 0)
				return new byte[0];

			byte[] bytes = e.GetBytes(str);
			return UrlEncodeToBytes(bytes, 0, bytes.Length);
		}

		public static byte[] UrlEncodeToBytes(byte[] bytes)
		{
			if (bytes == null)
				return null;

			if (bytes.Length == 0)
				return new byte[0];

			return UrlEncodeToBytes(bytes, 0, bytes.Length);
		}

		public static byte[] UrlEncodeToBytes(byte[] bytes, int offset, int count)
		{
			if (bytes == null)
				return null;
#if NET_4_0
			return MonoHttpEncoder.Current.UrlEncode(bytes, offset, count);
#else
			return HttpEncoder.UrlEncodeToBytes (bytes, offset, count);
#endif
		}

		public static string UrlEncodeUnicode(string str)
		{
			if (str == null)
				return null;

			return Encoding.ASCII.GetString(UrlEncodeUnicodeToBytes(str));
		}

		public static byte[] UrlEncodeUnicodeToBytes(string str)
		{
			if (str == null)
				return null;

			if (str.Length == 0)
				return new byte[0];

			MemoryStream result = new MemoryStream(str.Length);
			foreach (char c in str)
			{
				MonoHttpEncoder.UrlEncodeChar(c, result, true);
			}
			return result.ToArray();
		}
	}
}
