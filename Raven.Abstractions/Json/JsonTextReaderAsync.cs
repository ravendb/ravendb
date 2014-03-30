#region License
// Copyright (c) 2007 James Newton-King
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using System.Globalization;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Imports.Newtonsoft.Json
{
	/// <summary>
	/// Represents a reader that provides fast, non-cached, forward-only access to JSON text data.
	/// </summary>
	public class JsonTextReaderAsync : IJsonLineInfo, IDisposable
	{
		private const char UnicodeReplacementChar = '\uFFFD';

		private readonly TextReader _reader;
		private char[] _chars;
		private int _charsUsed;
		private int _charPos;
		private int _lineStartPos;
		private int _lineNumber;
		private bool _isEndOfFile;
		private StringBuffer _buffer;
		private StringReference _stringReference;

		/// <summary>
		/// Initializes a new instance of the <see cref="JsonReader"/> class with the specified <see cref="TextReader"/>.
		/// </summary>
		/// <param name="reader">The <c>TextReader</c> containing the XML data to read.</param>
		public JsonTextReaderAsync(TextReader reader)
		{
			_currentState = JsonReader.State.Start;
			_stack = new List<JsonPosition>(4);
			_dateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
			_dateParseHandling = DateParseHandling.None;

			CloseInput = true;
			if (reader == null)
				throw new ArgumentNullException("reader");

			_reader = reader;
			_lineNumber = 1;
			_chars = new char[4097];
		}

		internal void SetCharBuffer(char[] chars)
		{
			_chars = chars;
		}

		private StringBuffer GetBuffer()
		{
			if (_buffer == null)
			{
				_buffer = new StringBuffer(4096);
			}
			else
			{
				_buffer.Position = 0;
			}

			return _buffer;
		}

		private void OnNewLine(int pos)
		{
			_lineNumber++;
			_lineStartPos = pos - 1;
		}

		private async Task ParseString(char quote)
		{
			_charPos++;

			ShiftBufferIfNeeded();
			await ReadStringIntoBuffer(quote);

			if (_readType == ReadType.ReadAsBytes)
			{
				byte[] data;
				if (_stringReference.Length == 0)
				{
					data = new byte[0];
				}
				else
				{
					data = Convert.FromBase64CharArray(_stringReference.Chars, _stringReference.StartIndex, _stringReference.Length);
				}

				SetToken(JsonToken.Bytes, data);
			}
			else if (_readType == ReadType.ReadAsString)
			{
				string text = _stringReference.ToString();

				SetToken(JsonToken.String, text);
				QuoteChar = quote;
			}
			else
			{
				string text = _stringReference.ToString();

				if (_dateParseHandling != DateParseHandling.None)
				{
					if (text.Length > 0)
					{
						if (text[0] == '/')
						{
							if (text.StartsWith("/Date(", StringComparison.Ordinal) && text.EndsWith(")/", StringComparison.Ordinal))
							{
								ParseDateMicrosoft(text);
								return;
							}
						}
						else if (char.IsDigit(text[0]) && text.Length >= 19 && text.Length <= 40)
						{
							if (ParseDateIso(text))
								return;
						}
					}
				}

				SetToken(JsonToken.String, text);
				QuoteChar = quote;
			}
		}

		private bool ParseDateIso(string text)
		{
			const string isoDateFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFFK";

#if !NET20
			if (_readType == ReadType.ReadAsDateTimeOffset || (_readType == ReadType.Read && _dateParseHandling == DateParseHandling.DateTimeOffset))
			{
				DateTimeOffset dateTimeOffset;
				if (DateTimeOffset.TryParseExact(text, isoDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffset))
				{
					SetToken(JsonToken.Date, dateTimeOffset);
					return true;
				}
			}
			else
#endif
			{
				DateTime dateTime;
				if (DateTime.TryParseExact(text, isoDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
				{
					dateTime = DateTimeUtils.EnsureDateTime(dateTime, DateTimeZoneHandling);

					SetToken(JsonToken.Date, dateTime);
					return true;
				}
			}

			return false;
		}

		private void ParseDateMicrosoft(string text)
		{
			string value = text.Substring(6, text.Length - 8);
			DateTimeKind kind = DateTimeKind.Utc;

			int index = value.IndexOf('+', 1);

			if (index == -1)
				index = value.IndexOf('-', 1);

			TimeSpan offset = TimeSpan.Zero;

			if (index != -1)
			{
				kind = DateTimeKind.Local;
				offset = ReadOffset(value.Substring(index));
				value = value.Substring(0, index);
			}

			long javaScriptTicks = long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);

			DateTime utcDateTime = DateTimeUtils.ConvertJavaScriptTicksToDateTime(javaScriptTicks);

#if !NET20
			if (_readType == ReadType.ReadAsDateTimeOffset || (_readType == ReadType.Read && _dateParseHandling == DateParseHandling.DateTimeOffset))
			{
				SetToken(JsonToken.Date, new DateTimeOffset(utcDateTime.Add(offset).Ticks, offset));
			}
			else
#endif
			{
				DateTime dateTime;

				switch (kind)
				{
					case DateTimeKind.Unspecified:
						dateTime = DateTime.SpecifyKind(utcDateTime.ToLocalTime(), DateTimeKind.Unspecified);
						break;
					case DateTimeKind.Local:
						dateTime = utcDateTime.ToLocalTime();
						break;
					default:
						dateTime = utcDateTime;
						break;
				}

				dateTime = DateTimeUtils.EnsureDateTime(dateTime, DateTimeZoneHandling);

				SetToken(JsonToken.Date, dateTime);
			}
		}

		private static void BlockCopyChars(char[] src, int srcOffset, char[] dst, int dstOffset, int count)
		{
			const int charByteCount = 2;

			Buffer.BlockCopy(src, srcOffset * charByteCount, dst, dstOffset * charByteCount, count * charByteCount);
		}

		private void ShiftBufferIfNeeded()
		{
			// once in the last 10% of the buffer shift the remainling content to the start to avoid
			// unnessesarly increasing the buffer size when reading numbers/strings
			int length = _chars.Length;
			if (length - _charPos <= length * 0.1)
			{
				int count = _charsUsed - _charPos;
				if (count > 0)
					BlockCopyChars(_chars, _charPos, _chars, 0, count);

				_lineStartPos -= _charPos;
				_charPos = 0;
				_charsUsed = count;
				_chars[_charsUsed] = '\0';
			}
		}

		private Task<int> ReadData(bool append)
		{
			return ReadData(append, 0);
		}

		private async Task<int> ReadData(bool append, int charsRequired)
		{
			if (_isEndOfFile)
				return 0;

			// char buffer is full
			if (_charsUsed + charsRequired >= _chars.Length - 1)
			{
				if (append)
				{
					// copy to new array either double the size of the current or big enough to fit required content
					int newArrayLength = Math.Max(_chars.Length * 2, _charsUsed + charsRequired + 1);

					// increase the size of the buffer
					char[] dst = new char[newArrayLength];

					BlockCopyChars(_chars, 0, dst, 0, _chars.Length);

					_chars = dst;
				}
				else
				{
					int remainingCharCount = _charsUsed - _charPos;

					if (remainingCharCount + charsRequired + 1 >= _chars.Length)
					{
						// the remaining count plus the required is bigger than the current buffer size
						char[] dst = new char[remainingCharCount + charsRequired + 1];

						if (remainingCharCount > 0)
							BlockCopyChars(_chars, _charPos, dst, 0, remainingCharCount);

						_chars = dst;
					}
					else
					{
						// copy any remaining data to the beginning of the buffer if needed and reset positions
						if (remainingCharCount > 0)
							BlockCopyChars(_chars, _charPos, _chars, 0, remainingCharCount);
					}

					_lineStartPos -= _charPos;
					_charPos = 0;
					_charsUsed = remainingCharCount;
				}
			}

			int attemptCharReadCount = _chars.Length - _charsUsed - 1;

			int charsRead = await _reader.ReadAsync(_chars, _charsUsed, attemptCharReadCount);

			_charsUsed += charsRead;

			if (charsRead == 0)
				_isEndOfFile = true;

			_chars[_charsUsed] = '\0';
			return charsRead;
		}

		private async Task<bool> EnsureChars(int relativePosition, bool append)
		{
			if (_charPos + relativePosition >= _charsUsed)
				return await ReadChars(relativePosition, append);

			return true;
		}

		private async Task<bool> ReadChars(int relativePosition, bool append)
		{
			if (_isEndOfFile)
				return false;

			int charsRequired = _charPos + relativePosition - _charsUsed + 1;

			int totalCharsRead = 0;

			// it is possible that the TextReader doesn't return all data at once
			// repeat read until the required text is returned or the reader is out of content
			do
			{
				int charsRead = await ReadData(append, charsRequired - totalCharsRead);

				// no more content
				if (charsRead == 0)
					break;

				totalCharsRead += charsRead;
			}
			while (totalCharsRead < charsRequired);

			if (totalCharsRead < charsRequired)
				return false;
			return true;
		}

		private static TimeSpan ReadOffset(string offsetText)
		{
			bool negative = (offsetText[0] == '-');

			int hours = int.Parse(offsetText.Substring(1, 2), NumberStyles.Integer, CultureInfo.InvariantCulture);
			int minutes = 0;
			if (offsetText.Length >= 5)
				minutes = int.Parse(offsetText.Substring(3, 2), NumberStyles.Integer, CultureInfo.InvariantCulture);

			TimeSpan offset = TimeSpan.FromHours(hours) + TimeSpan.FromMinutes(minutes);
			if (negative)
				offset = offset.Negate();

			return offset;
		}

		/// <summary>
		/// Reads the next JSON token from the stream.
		/// </summary>
		/// <returns>
		/// true if the next token was read successfully; false if there are no more tokens to read.
		/// </returns>
		[DebuggerStepThrough]
		public async Task<bool> ReadAsync()
		{
			_readType = ReadType.Read;
			if (!await ReadInternal())
			{
				SetToken(JsonToken.None);
				return false;
			}

			return true;
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="T:Byte[]"/>.
		/// </summary>
		/// <returns>
		/// A <see cref="T:Byte[]"/> or a null reference if the next JSON token is null. This method will return <c>null</c> at the end of an array.
		/// </returns>
		public async Task<byte[]> ReadAsBytes()
		{
			return await ReadAsBytesInternal();
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{Decimal}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{Decimal}"/>. This method will return <c>null</c> at the end of an array.</returns>
		public  async Task<decimal?> ReadAsDecimal()
		{
			return await ReadAsDecimalInternal();
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{Int32}"/>.
		/// </summary>
		/// <returns>A <see cref="Nullable{Int32}"/>. This method will return <c>null</c> at the end of an array.</returns>
		public  async Task<int?> ReadAsInt32()
		{
			return await ReadAsInt32Internal();
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="String"/>.
		/// </summary>
		/// <returns>A <see cref="String"/>. This method will return <c>null</c> at the end of an array.</returns>
		public async Task<string> ReadAsString()
		{
			return await ReadAsStringInternal();
		}

		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{DateTime}"/>.
		/// </summary>
		/// <returns>A <see cref="String"/>. This method will return <c>null</c> at the end of an array.</returns>
		public async Task<DateTime?> ReadAsDateTime()
		{
			return await ReadAsDateTimeInternal();
		}

#if !NET20
		/// <summary>
		/// Reads the next JSON token from the stream as a <see cref="Nullable{DateTimeOffset}"/>.
		/// </summary>
		/// <returns>A <see cref="DateTimeOffset"/>. This method will return <c>null</c> at the end of an array.</returns>
		public async Task<DateTimeOffset?> ReadAsDateTimeOffset()
		{
			return await ReadAsDateTimeOffsetInternal();
		}
#endif

		internal async Task<bool> ReadInternal()
		{
			while (true)
			{
				switch (_currentState)
				{
					case JsonReader.State.Start:
					case JsonReader.State.Property:
					case JsonReader.State.Array:
					case JsonReader.State.ArrayStart:
					case JsonReader.State.Constructor:
					case JsonReader.State.ConstructorStart:
						return await ParseValue();
					case JsonReader.State.Complete:
						break;
					case JsonReader.State.Object:
					case JsonReader.State.ObjectStart:
						return await ParseObject();
					case JsonReader.State.PostValue:
						// returns true if it hits
						// end of object or array
						if (await ParsePostValue())
							return true;
						break;
					case JsonReader.State.Finished:
						if (await EnsureChars(0, false))
						{
							await EatWhitespace(false);
							if (_isEndOfFile)
							{
								return false;
							}
							if (_chars[_charPos] == '/')
							{
								await ParseComment();
								return true;
							}
							else
							{
								throw JsonReaderException.Create(this, Path, "Additional text encountered after finished reading JSON content: {0}.".FormatWith(CultureInfo.InvariantCulture, _chars[_charPos]), null);
							}
						}
						return false;
					case JsonReader.State.Closed:
						break;
					case JsonReader.State.Error:
						break;
					default:
						throw JsonReaderException.Create(this,Path, "Unexpected state: {0}.".FormatWith(CultureInfo.InvariantCulture, CurrentState), null);
				}
			}
		}

		private async Task ReadStringIntoBuffer(char quote)
		{
			int charPos = _charPos;
			int initialPosition = _charPos;
			int lastWritePosition = _charPos;
			StringBuffer buffer = null;

			while (true)
			{
				switch (_chars[charPos++])
				{
					case '\0':
						if (_charsUsed == charPos - 1)
						{
							charPos--;

							if (await ReadData(true) == 0)
							{
								_charPos = charPos;
								throw JsonReaderException.Create(this,Path, "Unterminated string. Expected delimiter: {0}.".FormatWith(CultureInfo.InvariantCulture, quote), null);
							}
						}
						break;
					case '\\':
						_charPos = charPos;
						if (! await EnsureChars(0, true))
						{
							_charPos = charPos;
							throw JsonReaderException.Create(this, Path, "Unterminated string. Expected delimiter: {0}.".FormatWith(CultureInfo.InvariantCulture, quote), null);
						}

						// start of escape sequence
						int escapeStartPos = charPos - 1;

						char currentChar = _chars[charPos];

						char writeChar;

						switch (currentChar)
						{
							case 'b':
								charPos++;
								writeChar = '\b';
								break;
							case 't':
								charPos++;
								writeChar = '\t';
								break;
							case 'n':
								charPos++;
								writeChar = '\n';
								break;
							case 'f':
								charPos++;
								writeChar = '\f';
								break;
							case 'r':
								charPos++;
								writeChar = '\r';
								break;
							case '\\':
								charPos++;
								writeChar = '\\';
								break;
							case '"':
							case '\'':
							case '/':
								writeChar = currentChar;
								charPos++;
								break;
							case 'u':
								charPos++;
								_charPos = charPos;
								writeChar = await ParseUnicode();

								if (StringUtils.IsLowSurrogate(writeChar))
								{
									// low surrogate with no preceding high surrogate; this char is replaced
									writeChar = UnicodeReplacementChar;
								}
								else if (StringUtils.IsHighSurrogate(writeChar))
								{
									bool anotherHighSurrogate;

									// loop for handling situations where there are multiple consecutive high surrogates
									do
									{
										anotherHighSurrogate = false;

										// potential start of a surrogate pair
										if (await EnsureChars(2, true) && _chars[_charPos] == '\\' && _chars[_charPos + 1] == 'u')
										{
											char highSurrogate = writeChar;

											_charPos += 2;
											writeChar = await ParseUnicode();

											if (StringUtils.IsLowSurrogate(writeChar))
											{
												// a valid surrogate pair!
											}
											else if (StringUtils.IsHighSurrogate(writeChar))
											{
												// another high surrogate; replace current and start check over
												highSurrogate = UnicodeReplacementChar;
												anotherHighSurrogate = true;
											}
											else
											{
												// high surrogate not followed by low surrogate; original char is replaced
												highSurrogate = UnicodeReplacementChar;
											}

											if (buffer == null)
												buffer = GetBuffer();

											WriteCharToBuffer(buffer, highSurrogate, lastWritePosition, escapeStartPos);
											lastWritePosition = _charPos;
										}
										else
										{
											// there are not enough remaining chars for the low surrogate or is not follow by unicode sequence
											// replace high surrogate and continue on as usual
											writeChar = UnicodeReplacementChar;
										}
									} while (anotherHighSurrogate);
								}

								charPos = _charPos;
								break;
							default:
								charPos++;
								_charPos = charPos;
								throw JsonReaderException.Create(this, Path, "Bad JSON escape sequence: {0}.".FormatWith(CultureInfo.InvariantCulture, @"\" + currentChar), null);
						}

						if (buffer == null)
							buffer = GetBuffer();

						WriteCharToBuffer(buffer, writeChar, lastWritePosition, escapeStartPos);

						lastWritePosition = charPos;
						break;
					case StringUtils.CarriageReturn:
						_charPos = charPos - 1;
						await ProcessCarriageReturn(true);
						charPos = _charPos;
						break;
					case StringUtils.LineFeed:
						_charPos = charPos - 1;
						ProcessLineFeed();
						charPos = _charPos;
						break;
					case '"':
					case '\'':
						if (_chars[charPos - 1] == quote)
						{
							charPos--;

							if (initialPosition == lastWritePosition)
							{
								_stringReference = new StringReference(_chars, initialPosition, charPos - initialPosition);
							}
							else
							{
								if (buffer == null)
									buffer = GetBuffer();

								if (charPos > lastWritePosition)
									buffer.Append(_chars, lastWritePosition, charPos - lastWritePosition);

								_stringReference = new StringReference(buffer.GetInternalBuffer(), 0, buffer.Position);
							}

							charPos++;
							_charPos = charPos;
							return;
						}
						break;
				}
			}
		}

		private void WriteCharToBuffer(StringBuffer buffer, char writeChar, int lastWritePosition, int writeToPosition)
		{
			if (writeToPosition > lastWritePosition)
			{
				buffer.Append(_chars, lastWritePosition, writeToPosition - lastWritePosition);
			}

			buffer.Append(writeChar);
		}

		private async Task<char> ParseUnicode()
		{
			char writeChar;
			if (await EnsureChars(4, true))
			{
				string hexValues = new string(_chars, _charPos, 4);
				char hexChar = Convert.ToChar(int.Parse(hexValues, NumberStyles.HexNumber, NumberFormatInfo.InvariantInfo));
				writeChar = hexChar;

				_charPos += 4;
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Unexpected end while parsing unicode character.", null);
			}
			return writeChar;
		}

		private async Task ReadNumberIntoBuffer()
		{
			int charPos = _charPos;

			while (true)
			{
				switch (_chars[charPos++])
				{
					case '\0':
						if (_charsUsed == charPos - 1)
						{
							charPos--;
							_charPos = charPos;
							if (await ReadData(true) == 0)
								return;
						}
						break;
					case '-':
					case '+':
					case 'a':
					case 'A':
					case 'b':
					case 'B':
					case 'c':
					case 'C':
					case 'd':
					case 'D':
					case 'e':
					case 'E':
					case 'f':
					case 'F':
					case 'x':
					case 'X':
					case '.':
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
					case '8':
					case '9':
						break;
					default:
						_charPos = charPos - 1;
						return;
				}
			}
		}

		private void ClearRecentString()
		{
			if (_buffer != null)
				_buffer.Position = 0;

			_stringReference = new StringReference();
		}

		private async Task<bool> ParsePostValue()
		{
			while (true)
			{
				char currentChar = _chars[_charPos];

				switch (currentChar)
				{
					case '\0':
						if (_charsUsed == _charPos)
						{
							if (await ReadData(false) == 0)
							{
								_currentState = JsonReader.State.Finished;
								return false;
							}
						}
						else
						{
							_charPos++;
						}
						break;
					case '}':
						_charPos++;
						SetToken(JsonToken.EndObject);
						return true;
					case ']':
						_charPos++;
						SetToken(JsonToken.EndArray);
						return true;
					case ')':
						_charPos++;
						SetToken(JsonToken.EndConstructor);
						return true;
					case '/':
						await ParseComment();
						return true;
					case ',':
						_charPos++;

						// finished parsing
						SetStateBasedOnCurrent();
						return false;
					case ' ':
					case StringUtils.Tab:
						// eat
						_charPos++;
						break;
					case StringUtils.CarriageReturn:
						await ProcessCarriageReturn(false);
						break;
					case StringUtils.LineFeed:
						ProcessLineFeed();
						break;
					default:
						if (char.IsWhiteSpace(currentChar))
						{
							// eat
							_charPos++;
						}
						else
						{
							throw JsonReaderException.Create(this, Path, "After parsing a value an unexpected character was encountered: {0}.".FormatWith(CultureInfo.InvariantCulture, currentChar), null);
						}
						break;
				}
			}
		}

		private async Task<bool> ParseObject()
		{
			while (true)
			{
				char currentChar = _chars[_charPos];

				switch (currentChar)
				{
					case '\0':
						if (_charsUsed == _charPos)
						{
							if (await ReadData(false) == 0)
								return false;
						}
						else
						{
							_charPos++;
						}
						break;
					case '}':
						SetToken(JsonToken.EndObject);
						_charPos++;
						return true;
					case '/':
						await ParseComment();
						return true;
					case StringUtils.CarriageReturn:
						await ProcessCarriageReturn(false);
						break;
					case StringUtils.LineFeed:
						ProcessLineFeed();
						break;
					case ' ':
					case StringUtils.Tab:
						// eat
						_charPos++;
						break;
					default:
						if (char.IsWhiteSpace(currentChar))
						{
							// eat
							_charPos++;
						}
						else
						{
							return await ParseProperty();
						}
						break;
				}
			}
		}

		private async Task<bool> ParseProperty()
		{
			char firstChar = _chars[_charPos];
			char quoteChar;

			if (firstChar == '"' || firstChar == '\'')
			{
				_charPos++;
				quoteChar = firstChar;
				ShiftBufferIfNeeded();
				await ReadStringIntoBuffer(quoteChar);
			}
			else if (ValidIdentifierChar(firstChar))
			{
				quoteChar = '\0';
				ShiftBufferIfNeeded();
				await ParseUnquotedProperty();
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Invalid property identifier character: {0}.".FormatWith(CultureInfo.InvariantCulture, _chars[_charPos]), null);
			}

			string propertyName = _stringReference.ToString();

			await EatWhitespace(false);

			if (_chars[_charPos] != ':')
				throw JsonReaderException.Create(this, Path, "Invalid character after parsing property name. Expected ':' but got: {0}.".FormatWith(CultureInfo.InvariantCulture, _chars[_charPos]), null);

			_charPos++;

			SetToken(JsonToken.PropertyName, propertyName);
			QuoteChar = quoteChar;
			ClearRecentString();

			return true;
		}

		private bool ValidIdentifierChar(char value)
		{
			return (char.IsLetterOrDigit(value) || value == '_' || value == '$');
		}

		private async Task ParseUnquotedProperty()
		{
			int initialPosition = _charPos;

			// parse unquoted property name until whitespace or colon
			while (true)
			{
				switch (_chars[_charPos])
				{
					case '\0':
						if (_charsUsed == _charPos)
						{
							if (await ReadData(true) == 0)
								throw JsonReaderException.Create(this, Path, "Unexpected end while parsing unquoted property name.", null);

							break;
						}

						_stringReference = new StringReference(_chars, initialPosition, _charPos - initialPosition);
						return;
					default:
						char currentChar = _chars[_charPos];

						if (ValidIdentifierChar(currentChar))
						{
							_charPos++;
							break;
						}
						else if (char.IsWhiteSpace(currentChar) || currentChar == ':')
						{
							_stringReference = new StringReference(_chars, initialPosition, _charPos - initialPosition);
							return;
						}

						throw JsonReaderException.Create(this, Path, "Invalid JavaScript property identifier character: {0}.".FormatWith(CultureInfo.InvariantCulture, currentChar), null);
				}
			}
		}

		private async Task<bool> ParseValue()
		{
			while (true)
			{
				char currentChar = _chars[_charPos];

				switch (currentChar)
				{
					case '\0':
						if (_charsUsed == _charPos)
						{
							if (await ReadData(false) == 0)
								return false;
						}
						else
						{
							_charPos++;
						}
						break;
					case '"':
					case '\'':
						await ParseString(currentChar);
						return true;
					case 't':
						await ParseTrue();
						return true;
					case 'f':
						await ParseFalse();
						return true;
					case 'n':
						if (await EnsureChars(1, true))
						{
							char next = _chars[_charPos + 1];

							if (next == 'u')
								await ParseNull();
							else if (next == 'e')
								await ParseConstructor();
							else
								throw JsonReaderException.Create(this, Path, "Unexpected character encountered while parsing value: {0}.".FormatWith(CultureInfo.InvariantCulture, _chars[_charPos]), null);
						}
						else
						{
							throw JsonReaderException.Create(this, Path, "Unexpected end.", null);
						}
						return true;
					case 'N':
						await ParseNumberNaN();
						return true;
					case 'I':
						await ParseNumberPositiveInfinity();
						return true;
					case '-':
						if (await EnsureChars(1, true) && _chars[_charPos + 1] == 'I')
							await ParseNumberNegativeInfinity();
						else
							await ParseNumber();
						return true;
					case '/':
						await ParseComment();
						return true;
					case 'u':
						await ParseUndefined();
						return true;
					case '{':
						_charPos++;
						SetToken(JsonToken.StartObject);
						return true;
					case '[':
						_charPos++;
						SetToken(JsonToken.StartArray);
						return true;
					case ']':
						_charPos++;
						SetToken(JsonToken.EndArray);
						return true;
					case ',':
						// don't increment position, the next call to read will handle comma
						// this is done to handle multiple empty comma values
						SetToken(JsonToken.Undefined);
						return true;
					case ')':
						_charPos++;
						SetToken(JsonToken.EndConstructor);
						return true;
					case StringUtils.CarriageReturn:
						await ProcessCarriageReturn(false);
						break;
					case StringUtils.LineFeed:
						ProcessLineFeed();
						break;
					case ' ':
					case StringUtils.Tab:
						// eat
						_charPos++;
						break;
					default:
						if (char.IsWhiteSpace(currentChar))
						{
							// eat
							_charPos++;
							break;
						}
						else if (char.IsNumber(currentChar) || currentChar == '-' || currentChar == '.')
						{
							await ParseNumber();
							return true;
						}
						else
						{
							throw JsonReaderException.Create(this, Path, "Unexpected character encountered while parsing value: {0}.".FormatWith(CultureInfo.InvariantCulture, currentChar), null);
						}
				}
			}
		}

		private void ProcessLineFeed()
		{
			_charPos++;
			OnNewLine(_charPos);
		}

		private async Task ProcessCarriageReturn(bool append)
		{
			_charPos++;

			if (await EnsureChars(1, append) && _chars[_charPos] == StringUtils.LineFeed)
				_charPos++;

			OnNewLine(_charPos);
		}

		private async Task<bool> EatWhitespace(bool oneOrMore)
		{
			bool finished = false;
			bool ateWhitespace = false;
			while (!finished)
			{
				char currentChar = _chars[_charPos];

				switch (currentChar)
				{
					case '\0':
						if (_charsUsed == _charPos)
						{
							if (await ReadData(false) == 0)
								finished = true;
						}
						else
						{
							_charPos++;
						}
						break;
					case StringUtils.CarriageReturn:
						await ProcessCarriageReturn(false);
						break;
					case StringUtils.LineFeed:
						ProcessLineFeed();
						break;
					default:
						if (currentChar == ' ' || char.IsWhiteSpace(currentChar))
						{
							ateWhitespace = true;
							_charPos++;
						}
						else
						{
							finished = true;
						}
						break;
				}
			}

			return (!oneOrMore || ateWhitespace);
		}

		private async Task ParseConstructor()
		{
			if (await MatchValueWithTrailingSeperator("new"))
			{
				await EatWhitespace(false);

				int initialPosition = _charPos;
				int endPosition;

				while (true)
				{
					char currentChar = _chars[_charPos];
					if (currentChar == '\0')
					{
						if (_charsUsed == _charPos)
						{
							if (await ReadData(true) == 0)
								throw JsonReaderException.Create(this, Path, "Unexpected end while parsing constructor.", null);
						}
						else
						{
							endPosition = _charPos;
							_charPos++;
							break;
						}
					}
					else if (char.IsLetterOrDigit(currentChar))
					{
						_charPos++;
					}
					else if (currentChar == StringUtils.CarriageReturn)
					{
						endPosition = _charPos;
						await ProcessCarriageReturn(true);
						break;
					}
					else if (currentChar == StringUtils.LineFeed)
					{
						endPosition = _charPos;
						ProcessLineFeed();
						break;
					}
					else if (char.IsWhiteSpace(currentChar))
					{
						endPosition = _charPos;
						_charPos++;
						break;
					}
					else if (currentChar == '(')
					{
						endPosition = _charPos;
						break;
					}
					else
					{
						throw JsonReaderException.Create(this, Path, "Unexpected character while parsing constructor: {0}.".FormatWith(CultureInfo.InvariantCulture, currentChar), null);
					}
				}

				_stringReference = new StringReference(_chars, initialPosition, endPosition - initialPosition);
				string constructorName = _stringReference.ToString();

				await EatWhitespace(false);

				if (_chars[_charPos] != '(')
					throw JsonReaderException.Create(this, Path, "Unexpected character while parsing constructor: {0}.".FormatWith(CultureInfo.InvariantCulture, _chars[_charPos]), null);

				_charPos++;

				ClearRecentString();

				SetToken(JsonToken.StartConstructor, constructorName);
			}
		}

		private async Task ParseNumber()
		{
			ShiftBufferIfNeeded();

			char firstChar = _chars[_charPos];
			int initialPosition = _charPos;

			await ReadNumberIntoBuffer();

			_stringReference = new StringReference(_chars, initialPosition, _charPos - initialPosition);

			object numberValue;
			JsonToken numberType;

			bool singleDigit = (char.IsDigit(firstChar) && _stringReference.Length == 1);
			bool nonBase10 = (firstChar == '0' && _stringReference.Length > 1
			  && _stringReference.Chars[_stringReference.StartIndex + 1] != '.'
			  && _stringReference.Chars[_stringReference.StartIndex + 1] != 'e'
			  && _stringReference.Chars[_stringReference.StartIndex + 1] != 'E');

			if (_readType == ReadType.ReadAsInt32)
			{
				if (singleDigit)
				{
					// digit char values start at 48
					numberValue = firstChar - 48;
				}
				else if (nonBase10)
				{
					string number = _stringReference.ToString();

					// decimal.Parse doesn't support parsing hexadecimal values
					int integer = number.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
									 ? Convert.ToInt32(number, 16)
									 : Convert.ToInt32(number, 8);

					numberValue = integer;
				}
				else
				{
					string number = _stringReference.ToString();

					numberValue = Convert.ToInt32(number, CultureInfo.InvariantCulture);
				}

				numberType = JsonToken.Integer;
			}
			else if (_readType == ReadType.ReadAsDecimal)
			{
				if (singleDigit)
				{
					// digit char values start at 48
					numberValue = (decimal)firstChar - 48;
				}
				else if (nonBase10)
				{
					string number = _stringReference.ToString();

					// decimal.Parse doesn't support parsing hexadecimal values
					long integer = number.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
									 ? Convert.ToInt64(number, 16)
									 : Convert.ToInt64(number, 8);

					numberValue = Convert.ToDecimal(integer);
				}
				else
				{
					string number = _stringReference.ToString();

					numberValue = decimal.Parse(number, NumberStyles.Number | NumberStyles.AllowExponent, CultureInfo.InvariantCulture);
				}

				numberType = JsonToken.Float;
			}
			else
			{
				if (singleDigit)
				{
					// digit char values start at 48
					numberValue = (long)firstChar - 48;
					numberType = JsonToken.Integer;
				}
				else if (nonBase10)
				{
					string number = _stringReference.ToString();

					numberValue = number.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
									? Convert.ToInt64(number, 16)
									: Convert.ToInt64(number, 8);
					numberType = JsonToken.Integer;
				}
				else
				{
					string number = _stringReference.ToString();

					// it's faster to do 3 indexof with single characters than an indexofany
					if (number.IndexOf('.') != -1 || number.IndexOf('E') != -1 || number.IndexOf('e') != -1)
					{
						numberValue = Convert.ToDouble(number, CultureInfo.InvariantCulture);
						numberType = JsonToken.Float;
					}
					else
					{
						try
						{
							numberValue = Convert.ToInt64(number, CultureInfo.InvariantCulture);
						}
						catch (OverflowException ex)
						{
							throw JsonReaderException.Create(this, Path, "JSON integer {0} is too large or small for an Int64.".FormatWith(CultureInfo.InvariantCulture, number), ex);
						}

						numberType = JsonToken.Integer;
					}
				}
			}

			ClearRecentString();

			SetToken(numberType, numberValue);
		}

		private async Task ParseComment()
		{
			// should have already parsed / character before reaching this method
			_charPos++;

			if (!await EnsureChars(1, false) || _chars[_charPos] != '*')
				throw JsonReaderException.Create(this, Path,"Error parsing comment. Expected: *, got {0}.".FormatWith(CultureInfo.InvariantCulture, _chars[_charPos]), null);
			else
				_charPos++;

			int initialPosition = _charPos;

			bool commentFinished = false;

			while (!commentFinished)
			{
				switch (_chars[_charPos])
				{
					case '\0':
						if (_charsUsed == _charPos)
						{
							if (await ReadData(true) == 0)
								throw JsonReaderException.Create(this, Path, "Unexpected end while parsing comment.", null);
						}
						else
						{
							_charPos++;
						}
						break;
					case '*':
						_charPos++;

						if (await EnsureChars(0, true))
						{
							if (_chars[_charPos] == '/')
							{
								_stringReference = new StringReference(_chars, initialPosition, _charPos - initialPosition - 1);

								_charPos++;
								commentFinished = true;
							}
						}
						break;
					case StringUtils.CarriageReturn:
						await ProcessCarriageReturn(true);
						break;
					case StringUtils.LineFeed:
						ProcessLineFeed();
						break;
					default:
						_charPos++;
						break;
				}
			}

			SetToken(JsonToken.Comment, _stringReference.ToString());

			ClearRecentString();
		}

		private async Task<bool> MatchValue(string value)
		{
			if (!await EnsureChars(value.Length - 1, true))
				return false;

			for (int i = 0; i < value.Length; i++)
			{
				if (_chars[_charPos + i] != value[i])
				{
					return false;
				}
			}

			_charPos += value.Length;

			return true;
		}

		private async Task<bool> MatchValueWithTrailingSeperator(string value)
		{
			// will match value and then move to the next character, checking that it is a seperator character
			bool match = await MatchValue(value);

			if (!match)
				return false;

			if (!await EnsureChars(0, false))
				return true;

			return await IsSeperator(_chars[_charPos]) || _chars[_charPos] == '\0';
		}

		private async Task<bool> IsSeperator(char c)
		{
			switch (c)
			{
				case '}':
				case ']':
				case ',':
					return true;
				case '/':
					// check next character to see if start of a comment
					if (!await EnsureChars(1, false))
						return false;

					return (_chars[_charPos + 1] == '*');
				case ')':
					if (CurrentState == JsonReader.State.Constructor || CurrentState == JsonReader.State.ConstructorStart)
						return true;
					break;
				case ' ':
				case StringUtils.Tab:
				case StringUtils.LineFeed:
				case StringUtils.CarriageReturn:
					return true;
				default:
					if (char.IsWhiteSpace(c))
						return true;
					break;
			}

			return false;
		}

		private async Task ParseTrue()
		{
			// check characters equal 'true'
			// and that it is followed by either a seperator character
			// or the text ends
			if (await MatchValueWithTrailingSeperator(JsonConvert.True))
			{
				SetToken(JsonToken.Boolean, true);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing boolean value.", null);
			}
		}

		private async Task ParseNull()
		{
			if (await MatchValueWithTrailingSeperator(JsonConvert.Null))
			{
				SetToken(JsonToken.Null);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing null value.", null);
			}
		}

		private async Task ParseUndefined()
		{
			if (await MatchValueWithTrailingSeperator(JsonConvert.Undefined))
			{
				SetToken(JsonToken.Undefined);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing undefined value.", null);
			}
		}

		private async Task ParseFalse()
		{
			if (await MatchValueWithTrailingSeperator(JsonConvert.False))
			{
				SetToken(JsonToken.Boolean, false);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing boolean value.", null);
			}
		}

		private async Task ParseNumberNegativeInfinity()
		{
			if (await MatchValueWithTrailingSeperator(JsonConvert.NegativeInfinity))
			{
				SetToken(JsonToken.Float, double.NegativeInfinity);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing negative infinity value.", null);
			}
		}

		private async Task ParseNumberPositiveInfinity()
		{
			if (await MatchValueWithTrailingSeperator(JsonConvert.PositiveInfinity))
			{
				SetToken(JsonToken.Float, double.PositiveInfinity);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing positive infinity value.", null);
			}
		}

		private async Task ParseNumberNaN()
		{
			if (await MatchValueWithTrailingSeperator(JsonConvert.NaN))
			{
				SetToken(JsonToken.Float, double.NaN);
			}
			else
			{
				throw JsonReaderException.Create(this, Path, "Error parsing NaN value.", null);
			}
		}

		/// <summary>
		/// Changes the state to closed. 
		/// </summary>
		public virtual void Close()
		{
			   _currentState = JsonReader.State.Closed;
      _tokenType = JsonToken.None;
      _value = null;

			if (CloseInput && _reader != null)
#if !(NETFX_CORE || PORTABLE)
				_reader.Close();
#else
        _reader.Dispose();
#endif

			if (_buffer != null)
				_buffer.Clear();
		}

		/// <summary>
		/// Gets a value indicating whether the class can return line information.
		/// </summary>
		/// <returns>
		/// 	<c>true</c> if LineNumber and LinePosition can be provided; otherwise, <c>false</c>.
		/// </returns>
		public bool HasLineInfo()
		{
			return true;
		}

		/// <summary>
		/// Gets the current line number.
		/// </summary>
		/// <value>
		/// The current line number or 0 if no line information is available (for example, HasLineInfo returns false).
		/// </value>
		public int LineNumber
		{
			get
			{
				if (CurrentState == JsonReader.State.Start && LinePosition == 0)
					return 0;

				return _lineNumber;
			}
		}

		/// <summary>
		/// Gets the current line position.
		/// </summary>
		/// <value>
		/// The current line position or 0 if no line information is available (for example, HasLineInfo returns false).
		/// </value>
		public int LinePosition
		{
			get { return _charPos - _lineStartPos; }
		}


		  // current Token data
    private JsonToken _tokenType;
    private object _value;
    private char _quoteChar;
    internal JsonReader.State _currentState;
    internal ReadType _readType;
    private JsonPosition _currentPosition;
    private CultureInfo _culture;
    private DateTimeZoneHandling _dateTimeZoneHandling;
    private int? _maxDepth;
    private bool _hasExceededMaxDepth;
    internal DateParseHandling _dateParseHandling;
    private readonly List<JsonPosition> _stack;

    /// <summary>
    /// Gets the current reader JsonReader.State.
    /// </summary>
    /// <value>The current reader JsonReader.State.</value>
    internal JsonReader.State CurrentState
    {
      get { return _currentState; }
    }

    /// <summary>
    /// Gets or sets a value indicating whether the underlying stream or
    /// <see cref="TextReader"/> should be closed when the reader is closed.
    /// </summary>
    /// <value>
    /// true to close the underlying stream or <see cref="TextReader"/> when
    /// the reader is closed; otherwise false. The default is true.
    /// </value>
    public bool CloseInput { get; set; }

    /// <summary>
    /// Gets the quotation mark character used to enclose the value of a string.
    /// </summary>
    public virtual char QuoteChar
    {
      get { return _quoteChar; }
      protected internal set { _quoteChar = value; }
    }

    /// <summary>
    /// Get or set how <see cref="DateTime"/> time zones are handling when reading JSON.
    /// </summary>
    public DateTimeZoneHandling DateTimeZoneHandling
    {
      get { return _dateTimeZoneHandling; }
      set { _dateTimeZoneHandling = value; }
    }

    /// <summary>
    /// Get or set how date formatted strings, e.g. "\/Date(1198908717056)\/" and "2012-03-21T05:40Z", are parsed when reading JSON.
    /// </summary>
    public DateParseHandling DateParseHandling
    {
      get { return _dateParseHandling; }
      set { _dateParseHandling = value; }
    }

    /// <summary>
    /// Gets or sets the maximum depth allowed when reading JSON. Reading past this depth will throw a <see cref="JsonReaderException"/>.
    /// </summary>
    public int? MaxDepth
    {
      get { return _maxDepth; }
      set
      {
        if (value <= 0)
          throw new ArgumentException("Value must be positive.", "value");

        _maxDepth = value;
      }
    }

    /// <summary>
    /// Gets the type of the current JSON token. 
    /// </summary>
    public virtual JsonToken TokenType
    {
      get { return _tokenType; }
    }

    /// <summary>
    /// Gets the text value of the current JSON token.
    /// </summary>
    public virtual object Value
    {
      get { return _value; }
    }

    /// <summary>
    /// Gets The Common Language Runtime (CLR) type for the current JSON token.
    /// </summary>
    public virtual Type ValueType
    {
      get { return (_value != null) ? _value.GetType() : null; }
    }

    /// <summary>
    /// Gets the depth of the current token in the JSON document.
    /// </summary>
    /// <value>The depth of the current token in the JSON document.</value>
    public virtual int Depth
    {
      get
      {
        int depth = _stack.Count;
        if (IsStartToken(TokenType) || _currentPosition.Type == JsonContainerType.None)
          return depth;
        else
          return depth + 1;
      }
    }

    /// <summary>
    /// Gets the path of the current JSON token. 
    /// </summary>
    public virtual string Path
    {
      get
      {
        if (_currentPosition.Type == JsonContainerType.None)
          return string.Empty;

        bool insideContainer = (_currentState != JsonReader.State.ArrayStart
          && _currentState != JsonReader.State.ConstructorStart
          && _currentState != JsonReader.State.ObjectStart);

        IEnumerable<JsonPosition> positions = (!insideContainer)
          ? _stack
          : _stack.Concat(new[] {_currentPosition});

        return JsonPosition.BuildPath(positions);
      }
    }

    /// <summary>
    /// Gets or sets the culture used when reading JSON. Defaults to <see cref="CultureInfo.InvariantCulture"/>.
    /// </summary>
    public CultureInfo Culture
    {
      get { return _culture ?? CultureInfo.InvariantCulture; }
      set { _culture = value; }
    }

    internal JsonPosition GetPosition(int depth)
    {
      if (depth < _stack.Count)
        return _stack[depth];

      return _currentPosition;
    }



    private void Push(JsonContainerType value)
    {
      UpdateScopeWithFinishedValue();

      if (_currentPosition.Type == JsonContainerType.None)
      {
        _currentPosition = new JsonPosition(value);
      }
      else
      {
        _stack.Add(_currentPosition);
        _currentPosition = new JsonPosition(value);

        // this is a little hacky because Depth increases when first property/value is written but only testing here is faster/simpler
        if (_maxDepth != null && Depth + 1 > _maxDepth && !_hasExceededMaxDepth)
        {
          _hasExceededMaxDepth = true;
          throw JsonReaderException.Create(this, Path, "The reader's MaxDepth of {0} has been exceeded.".FormatWith(CultureInfo.InvariantCulture, _maxDepth), null);
        }
      }
    }

    private JsonContainerType Pop()
    {
      JsonPosition oldPosition;
      if (_stack.Count > 0)
      {
        oldPosition = _currentPosition;
        _currentPosition = _stack[_stack.Count - 1];
        _stack.RemoveAt(_stack.Count - 1);
      }
      else
      {
        oldPosition = _currentPosition;
        _currentPosition = new JsonPosition();
      }

      if (_maxDepth != null && Depth <= _maxDepth)
        _hasExceededMaxDepth = false;

      return oldPosition.Type;
    }

    private JsonContainerType Peek()
    {
      return _currentPosition.Type;
    }

	
#if !NET20
    internal async Task<DateTimeOffset?> ReadAsDateTimeOffsetInternal()
    {
      _readType = ReadType.ReadAsDateTimeOffset;

      do
      {
        if (!await ReadInternal())
        {
          SetToken(JsonToken.None);
          return null;
        }
      } while (TokenType == JsonToken.Comment);

      if (TokenType == JsonToken.Date)
      {
        if (Value is DateTime)
          SetToken(JsonToken.Date, new DateTimeOffset((DateTime)Value));

        return (DateTimeOffset)Value;
      }

      if (TokenType == JsonToken.Null)
        return null;

      DateTimeOffset dt;
      if (TokenType == JsonToken.String)
      {
        string s = (string)Value;
        if (string.IsNullOrEmpty(s))
        {
          SetToken(JsonToken.Null);
          return null;
        }

        if (DateTimeOffset.TryParse(s, Culture, DateTimeStyles.RoundtripKind, out dt))
        {
          SetToken(JsonToken.Date, dt);
          return dt;
        }
        else
        {
          throw JsonReaderException.Create(this, Path, "Could not convert string to DateTimeOffset: {0}.".FormatWith(CultureInfo.InvariantCulture, Value), null);
        }
      }

      if (TokenType == JsonToken.EndArray)
        return null;

      throw JsonReaderException.Create(this, Path, "Error reading date. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
    }
#endif

    internal async Task<byte[]> ReadAsBytesInternal()
    {
      _readType = ReadType.ReadAsBytes;

      do
      {
        if (!await ReadInternal())
        {
          SetToken(JsonToken.None);
          return null;
        }
      } while (TokenType == JsonToken.Comment);

      if (await IsWrappedInTypeObject())
      {
        byte[] data = await ReadAsBytes();
        await ReadInternal();
        SetToken(JsonToken.Bytes, data);
        return data;
      }

      // attempt to convert possible base 64 string to bytes
      if (TokenType == JsonToken.String)
      {
        string s = (string)Value;
        byte[] data = (s.Length == 0) ? new byte[0] : Convert.FromBase64String(s);
        SetToken(JsonToken.Bytes, data);
      }

      if (TokenType == JsonToken.Null)
        return null;

      if (TokenType == JsonToken.Bytes)
        return (byte[])Value;

      if (TokenType == JsonToken.StartArray)
      {
        List<byte> data = new List<byte>();

        while (await ReadInternal())
        {
          switch (TokenType)
          {
            case JsonToken.Integer:
              data.Add(Convert.ToByte(Value, CultureInfo.InvariantCulture));
              break;
            case JsonToken.EndArray:
              byte[] d = data.ToArray();
              SetToken(JsonToken.Bytes, d);
              return d;
            case JsonToken.Comment:
              // skip
              break;
            default:
              throw JsonReaderException.Create(this, Path, "Unexpected token when reading bytes: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
          }
        }

        throw JsonReaderException.Create(this, Path, "Unexpected end when reading bytes.", null);
      }

      if (TokenType == JsonToken.EndArray)
        return null;

      throw JsonReaderException.Create(this, Path, "Error reading bytes. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
    }

    internal async Task<decimal?> ReadAsDecimalInternal()
    {
      _readType = ReadType.ReadAsDecimal;

      do
      {
        if (!await ReadInternal())
        {
          SetToken(JsonToken.None);
          return null;
        }
      } while (TokenType == JsonToken.Comment);

      if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
      {
        if (!(Value is decimal))
          SetToken(JsonToken.Float, Convert.ToDecimal(Value, CultureInfo.InvariantCulture));

        return (decimal)Value;
      }

      if (TokenType == JsonToken.Null)
        return null;

      decimal d;
      if (TokenType == JsonToken.String)
      {
        string s = (string)Value;
        if (string.IsNullOrEmpty(s))
        {
          SetToken(JsonToken.Null);
          return null;
        }

        if (decimal.TryParse(s, NumberStyles.Number, Culture, out d))
        {
          SetToken(JsonToken.Float, d);
          return d;
        }
        else
        {
          throw JsonReaderException.Create(this, Path, "Could not convert string to decimal: {0}.".FormatWith(CultureInfo.InvariantCulture, Value), null);
        }
      }

      if (TokenType == JsonToken.EndArray)
        return null;

      throw JsonReaderException.Create(this, Path, "Error reading decimal. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
    }

    internal async Task<int?> ReadAsInt32Internal()
    {
      _readType = ReadType.ReadAsInt32;

      do
      {
        if (!await ReadInternal())
        {
          SetToken(JsonToken.None);
          return null;
        }
      } while (TokenType == JsonToken.Comment);

      if (TokenType == JsonToken.Integer || TokenType == JsonToken.Float)
      {
        if (!(Value is int))
          SetToken(JsonToken.Integer, Convert.ToInt32(Value, CultureInfo.InvariantCulture));

        return (int)Value;
      }

      if (TokenType == JsonToken.Null)
        return null;

      int i;
      if (TokenType == JsonToken.String)
      {
        string s = (string)Value;
        if (string.IsNullOrEmpty(s))
        {
          SetToken(JsonToken.Null);
          return null;
        }

        if (int.TryParse(s, NumberStyles.Integer, Culture, out i))
        {
          SetToken(JsonToken.Integer, i);
          return i;
        }
        else
        {
          throw JsonReaderException.Create(this, Path, "Could not convert string to integer: {0}.".FormatWith(CultureInfo.InvariantCulture, Value), null);
        }
      }

      if (TokenType == JsonToken.EndArray)
        return null;

      throw JsonReaderException.Create(this, Path, "Error reading integer. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
    }

    internal async Task<string> ReadAsStringInternal()
    {
      _readType = ReadType.ReadAsString;

      do
      {
        if (!await ReadInternal())
        {
          SetToken(JsonToken.None);
          return null;
        }
      } while (TokenType == JsonToken.Comment);

      if (TokenType == JsonToken.String)
        return (string)Value;

      if (TokenType == JsonToken.Null)
        return null;

      if (IsPrimitiveToken(TokenType))
      {
        if (Value != null)
        {
          string s;
          if (Value is IFormattable)
            s = ((IFormattable)Value).ToString(null, Culture);
          else
            s = Value.ToString();

          SetToken(JsonToken.String, s);
          return s;
        }
      }

      if (TokenType == JsonToken.EndArray)
        return null;

      throw JsonReaderException.Create(this, Path, "Error reading string. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
    }

    internal async Task<DateTime?> ReadAsDateTimeInternal()
    {
      _readType = ReadType.ReadAsDateTime;

      do
      {
        if (!await ReadInternal())
        {
          SetToken(JsonToken.None);
          return null;
        }
      } while (TokenType == JsonToken.Comment);

      if (TokenType == JsonToken.Date)
        return (DateTime)Value;

      if (TokenType == JsonToken.Null)
        return null;

      DateTime dt;
      if (TokenType == JsonToken.String)
      {
        string s = (string)Value;
        if (string.IsNullOrEmpty(s))
        {
          SetToken(JsonToken.Null);
          return null;
        }

        if (DateTime.TryParse(s, Culture, DateTimeStyles.RoundtripKind, out dt))
        {
          dt = DateTimeUtils.EnsureDateTime(dt, DateTimeZoneHandling);
          SetToken(JsonToken.Date, dt);
          return dt;
        }
        else
        {
          throw JsonReaderException.Create(this, Path, "Could not convert string to DateTime: {0}.".FormatWith(CultureInfo.InvariantCulture, Value), null);
        }
      }

      if (TokenType == JsonToken.EndArray)
        return null;

      throw JsonReaderException.Create(this, Path, "Error reading date. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, TokenType), null);
    }

    private async Task<bool> IsWrappedInTypeObject()
    {
      _readType = ReadType.Read;

      if (TokenType == JsonToken.StartObject)
      {
        if (!await ReadInternal())
          throw JsonReaderException.Create(this, Path, "Unexpected end when reading bytes.", null);

        if (Value.ToString() == "$type")
        {
          await ReadInternal();
          if (Value != null && Value.ToString().StartsWith("System.Byte[]"))
          {
            await ReadInternal();
            if (Value.ToString() == "$value")
            {
              return true;
            }
          }
        }

        throw JsonReaderException.Create(this, Path, "Error reading bytes. Unexpected token: {0}.".FormatWith(CultureInfo.InvariantCulture, JsonToken.StartObject), null);
      }

      return false;
    }

    /// <summary>
    /// Skips the children of the current token.
    /// </summary>
    public async Task SkipAsync()
    {
      if (TokenType == JsonToken.PropertyName)
        await ReadAsync();

      if (IsStartToken(TokenType))
      {
        int depth = Depth;

        while (await ReadAsync() && (depth < Depth))
        {
        }
      }
    }

    /// <summary>
    /// Sets the current token.
    /// </summary>
    /// <param name="newToken">The new token.</param>
    protected void SetToken(JsonToken newToken)
    {
      SetToken(newToken, null);
    }

    /// <summary>
    /// Sets the current token and value.
    /// </summary>
    /// <param name="newToken">The new token.</param>
    /// <param name="value">The value.</param>
    protected void SetToken(JsonToken newToken, object value)
    {
      _tokenType = newToken;
      _value = value;

      switch (newToken)
      {
        case JsonToken.StartObject:
          _currentState = JsonReader.State.ObjectStart;
          Push(JsonContainerType.Object);
          break;
        case JsonToken.StartArray:
          _currentState = JsonReader.State.ArrayStart;
          Push(JsonContainerType.Array);
          break;
        case JsonToken.StartConstructor:
          _currentState = JsonReader.State.ConstructorStart;
          Push(JsonContainerType.Constructor);
          break;
        case JsonToken.EndObject:
          ValidateEnd(JsonToken.EndObject);
          break;
        case JsonToken.EndArray:
          ValidateEnd(JsonToken.EndArray);
          break;
        case JsonToken.EndConstructor:
          ValidateEnd(JsonToken.EndConstructor);
          break;
        case JsonToken.PropertyName:
          _currentState = JsonReader.State.Property;

          _currentPosition.PropertyName = (string) value;
          break;
        case JsonToken.Undefined:
        case JsonToken.Integer:
        case JsonToken.Float:
        case JsonToken.Boolean:
        case JsonToken.Null:
        case JsonToken.Date:
        case JsonToken.String:
        case JsonToken.Raw:
        case JsonToken.Bytes:
          _currentState = (Peek() != JsonContainerType.None) ? JsonReader.State.PostValue : JsonReader.State.Finished;

          UpdateScopeWithFinishedValue();
          break;
      }
    }

    private void UpdateScopeWithFinishedValue()
    {
      if (_currentPosition.HasIndex)
        _currentPosition.Position++;
    }

    private void ValidateEnd(JsonToken endToken)
    {
      JsonContainerType currentObject = Pop();

      if (GetTypeForCloseToken(endToken) != currentObject)
        throw JsonReaderException.Create(this, Path, "JsonToken {0} is not valid for closing JsonType {1}.".FormatWith(CultureInfo.InvariantCulture, endToken, currentObject), null);

      _currentState = (Peek() != JsonContainerType.None) ? JsonReader.State.PostValue : JsonReader.State.Finished;
    }

    /// <summary>
    /// Sets the state based on current token type.
    /// </summary>
    protected void SetStateBasedOnCurrent()
    {
      JsonContainerType currentObject = Peek();

      switch (currentObject)
      {
        case JsonContainerType.Object:
          _currentState = JsonReader.State.Object;
          break;
        case JsonContainerType.Array:
          _currentState = JsonReader.State.Array;
          break;
        case JsonContainerType.Constructor:
          _currentState = JsonReader.State.Constructor;
          break;
        case JsonContainerType.None:
          _currentState = JsonReader.State.Finished;
          break;
        default:
          throw JsonReaderException.Create(this, Path, "While setting the reader state back to current object an unexpected JsonType was encountered: {0}".FormatWith(CultureInfo.InvariantCulture, currentObject), null);
      }
    }

    internal static bool IsPrimitiveToken(JsonToken token)
    {
      switch (token)
      {
        case JsonToken.Integer:
        case JsonToken.Float:
        case JsonToken.String:
        case JsonToken.Boolean:
        case JsonToken.Undefined:
        case JsonToken.Null:
        case JsonToken.Date:
        case JsonToken.Bytes:
          return true;
        default:
          return false;
      }
    }

    internal static bool IsStartToken(JsonToken token)
    {
      switch (token)
      {
        case JsonToken.StartObject:
        case JsonToken.StartArray:
        case JsonToken.StartConstructor:
          return true;
        default:
          return false;
      }
    }

    private JsonContainerType GetTypeForCloseToken(JsonToken token)
    {
      switch (token)
      {
        case JsonToken.EndObject:
          return JsonContainerType.Object;
        case JsonToken.EndArray:
          return JsonContainerType.Array;
        case JsonToken.EndConstructor:
          return JsonContainerType.Constructor;
        default:
          throw JsonReaderException.Create(this, Path, "Not a valid close JsonToken: {0}".FormatWith(CultureInfo.InvariantCulture, token), null);
      }
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
      Dispose(true);
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
      if (_currentState != JsonReader.State.Closed && disposing)
        Close();
    }

	}
}