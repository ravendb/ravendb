package net.ravendb.client.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import net.ravendb.abstractions.basic.Reference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class UrlUtils {

  public static final Character DUMMY_CHAR = 0xFFFF;

  private static Log log = LogFactory.getLog(UrlUtils.class.getCanonicalName());

  public static String escapeDataString(String stringToEscape) {
    if (stringToEscape == null) {
      throw new IllegalArgumentException("String is null");
    }

    if (stringToEscape.length() == 0) {
      return "";
    }

    Reference<Integer> position = new Reference<>(0);
    char[] dest = escapeString(stringToEscape, 0, stringToEscape.length(), null, position, false);
    if (dest == null) {
      return stringToEscape;
    }
    return new String(dest, 0, position.value);
  }

  private static void escapeAsciiChar(char ch, char[] to, Reference<Integer> posRef) {
    to[posRef.value++] = '%';
    to[posRef.value++] = HEX_UPPER_CHARS[(ch & 0xf0) >> 4];
    to[posRef.value++] = HEX_UPPER_CHARS[ch & 0xf];
  }


  private static final char[] HEX_UPPER_CHARS = {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
  };

  private final static short c_MaxAsciiCharsReallocate   = 40;
  private final static short c_MaxUnicodeCharsReallocate = 40;
  private final static short c_MaxUTF_8BytesPerUnicodeChar  = 4;
  private final static short c_EncodedCharsPerByte = 3;

  private static char[] escapeString(String input, int start, int end, char[] dest,
    Reference<Integer> destPos, boolean isUriString) {

    int i = start;
    int prevInputPos = start;
    byte[] bytes = new byte[c_MaxAsciiCharsReallocate * c_MaxUTF_8BytesPerUnicodeChar]; // 40*4=160

    String pStr = input;
    for (; i < end; ++i) {
      char ch = pStr.charAt(i);

      // a Unicode ?
      if (ch > 0x7F) {
        short maxSize = (short)Math.min(end - i, c_MaxUnicodeCharsReallocate - 1);

        short count = 1;
        for (; count < maxSize && pStr.charAt(i + count) > 0x7F; ++count) {
        }
        // Is the last a high surrogate?
        if (pStr.charAt(i + count-1) >= 0xD800 && pStr.charAt(i + count-1) <= 0xDBFF) {
          // Should be a rare case where the app tries to feed an invalid Unicode surrogates pair
          if (count == 1 || count == end - i)
            throw new RuntimeException(input + ": BAD_STRING");
          // need to grab one more char as a Surrogate except when it's a bogus input
          ++count;
        }
        dest = ensureDestinationSize(pStr, dest, i, (short)(count * c_MaxUTF_8BytesPerUnicodeChar*c_EncodedCharsPerByte),
          c_MaxUnicodeCharsReallocate*c_MaxUTF_8BytesPerUnicodeChar*c_EncodedCharsPerByte,
          destPos, prevInputPos);

        String substring = pStr.substring(i, i + count);
        byte[] subStringBytes = substring.getBytes();
        System.arraycopy(subStringBytes, 0, bytes, 0, subStringBytes.length);

        short numberOfBytes =  (short) subStringBytes.length;

        // This is the only exception that built in UriParser can throw after a Uri ctor.
        // Should not happen unless the app tries to feed an invalid Unicode String
        if (numberOfBytes == 0)
          throw new RuntimeException(input + ": BAD_STRING");

        i += (count-1);

        for (count = 0 ; count < numberOfBytes; ++count)
          escapeAsciiChar((char)bytes[count], dest, destPos);

        prevInputPos = i+1;
      } else if (isUriString? isNotReservedNotUnreservedNotHash(ch): isNotUnreserved(ch)) {
        dest = ensureDestinationSize(pStr, dest, i, c_EncodedCharsPerByte, c_MaxAsciiCharsReallocate*c_EncodedCharsPerByte, destPos, prevInputPos);
        escapeAsciiChar(ch, dest, destPos);
        prevInputPos = i+1;
      }


    }

    if (prevInputPos != i)
    {
      // need to fill up the dest array ?
      if (prevInputPos != start || dest != null)
        dest = ensureDestinationSize(pStr, dest, i, (short)0, 0, destPos, prevInputPos);
    }

    return dest;

  }


  private static char[] ensureDestinationSize(String pStr, char[] dest, int currentInputPos, short charsToAdd, int minReallocateChars,
    Reference<Integer> destPos, int prevInputPos) {
    if (dest == null || dest.length < destPos.value + (currentInputPos - prevInputPos) + charsToAdd) {
      // allocating or reallocating array by ensuring enough space based on maxCharsToAdd.
      char[] newresult = new char[destPos.value + (currentInputPos-prevInputPos) + minReallocateChars];
      if (dest != null && destPos.value != 0) {
        System.arraycopy(dest, 0, newresult, 0, Math.min(dest.length, newresult.length));
      }
      dest = newresult;
    }
    // ensuring we copied everything form the input string left before last escaping
    while (prevInputPos != currentInputPos)
      dest[destPos.value++] = pStr.charAt(prevInputPos++);
    return dest;

  }


  //
  // mark        = "-" | "_" | "." | "!" | "~" | "*" | "'" | "(" | ")"
  // reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" | "$" | ","
  // excluded = control | space | delims | unwise
  // delims      = "<" | ">" | "#" | "%" | <">
  // unwise      = "{" | "}" | "|" | "\" | "^" | "[" | "]" | "`"
  //
  private static boolean isNotReservedNotUnreservedNotHash(char c)
  {
      if (c > 'z' && c != '~')
      {
          return true;
      }
      else if (c > 'Z' && c < 'a' && c != '_')
      {
          return true;
      }
      else if (c < '!')
      {
          return true;
      }
      else if (c == '>' || c == '<' || c == '%' || c == '"' || c == '`')
      {
          return true;
      }
      return false;
  }

  private static boolean isNotUnreserved(char c)
  {
      if (c > 'z' && c != '~')
      {
          return true;
      }
      else if ((c > '9' && c < 'A') || (c > 'Z' && c < 'a' && c != '_'))
      {
          return true;
      }
      else if (c < '\'' && c != '!')
      {
          return true;
      }
      else if (c == '+' || c == ',' || c == '/')
      {
          return true;
      }
      return false;
  }


  public static String escapeUriString(String stringToEscape) {
    if (stringToEscape == null) {
      throw new IllegalArgumentException("String is null");
    }

    if (stringToEscape.length() == 0) {
      return "";
    }

    Reference<Integer> position = new Reference<>(0);
    char[] dest = escapeString(stringToEscape, 0, stringToEscape.length(), null, position, true);
    if (dest == null) {
      return stringToEscape;
    }
    return new String(dest, 0, position.value);
  }

  public static String unescapeDataString(String input) {
    try {
      if (input == null) {
        return null;
      }
      return URLDecoder.decode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error(e.getMessage(), e);
      return null;
    }

  }
}
