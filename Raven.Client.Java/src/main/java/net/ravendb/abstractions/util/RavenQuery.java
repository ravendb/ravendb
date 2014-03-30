package net.ravendb.abstractions.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * Helper class that provide a way to escape query terms
 */
public class RavenQuery {
  /**
   * Escapes Lucene operators and quotes phrases
   * @param term
   * @return
   */
  public static String escape(String term) {
    return escape(term, false, true);
  }

  /**
   * Escapes Lucene operators and quotes phrases
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
   * @param term
   * @param allowWildcards
   * @param makePhrase
   * @return
   */
  public static String escape(String term, boolean allowWildcards, boolean makePhrase) {
    // method doesn't allocate a StringBuilder unless the string requires escaping
    // also this copies chunks of the original string into the StringBuilder which
    // is far more efficient than copying character by character because StringBuilder
    // can access the underlying string data directly

    if (StringUtils.isEmpty(term)) {
      return "\"\"";
    }

    boolean isPhrase = false;
    int start = 0;
    int length = term.length();
    StringBuilder buffer = null;

    for (int i = start; i < length; i++) {
      char ch = term.charAt(i);
      switch (ch)
      {
        // should wildcards be included or excluded here?
        case '*':
        case '?':
            if (allowWildcards) {
              break;
            }
            //$FALL-THROUGH$
        case '+':
        case '-':
        case '&':
        case '|':
        case '!':
        case '(':
        case ')':
        case '{':
        case '}':
        case '[':
        case ']':
        case '^':
        case '"':
        case '~':
        case ':':
        case '\\':
          {
            if (buffer == null) {
              // allocate builder with headroom
              buffer = new StringBuilder(length * 2);
            }

            if (i > start) {
              // append any leading substring
              buffer.append(term, start, i);
            }

            buffer.append('\\').append(ch);
            start = i + 1;
            break;
          }
        case ' ':
        case '\t':
          {
            if (!isPhrase && makePhrase) {
              if (buffer == null) {
                // allocate builder with headroom
                buffer = new StringBuilder(length * 2);
              }

              buffer.insert(0, "\"");
              isPhrase = true;
            }
            break;
          }
      }
    }

    if (buffer == null) {
      if (makePhrase == false) {
        return term;
      }
      // no changes required
      switch (term) {
        case "OR":
          return "\"OR\"";
        case "AND":
          return "\"AND\"";
        case "NOT":
          return "\"NOT\"";
        default:
          return term;
      }
    }

    if (length > start) {
      // append any trailing substring
      buffer.append(term, start, length);
    }

    if (isPhrase) {
      // quoted phrase
      buffer.append('"');
    }

    return buffer.toString();
  }

  private static final Set<Character> fieldChars = new HashSet<>(Arrays.asList('*', '?', '+', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"',  '~', '\\', ':', ' ', '\t'));

  /**
   * Escapes Lucene field
   * @param field
   * @return
   */
  public static String escapeField(String field) {
    // method doesn't allocate a StringBuilder unless the string requires escaping
    // also this copies chunks of the original string into the StringBuilder which
    // is far more efficient than copying character by character because StringBuilder
    // can access the underlying string data directly

    if (StringUtils.isEmpty(field)) {
      return "\"\"";
    }

    int start = 0;
    int length = field.length();
    StringBuilder buffer = null;

    for (int i = start; i < length; i++) {
      char ch = field.charAt(i);

      if (ch == '\\') {
        if (i + 1 < length && fieldChars.contains(field.charAt(i+1))) {
          i++; // skip next, since it was escaped
          continue;
        }
      } else if (!fieldChars.contains(ch))
        continue;

      if (buffer == null) {
        // allocate builder with headroom
        buffer = new StringBuilder(length * 2);
      }

      if (i > start) {
        // append any leading substring
        buffer.append(field, start, i);
      }

      buffer.append('\\').append(ch);
      start = i + 1;
    }

    if (buffer == null) {
      return field;
    }

    if (length > start) {
      // append any trailing substring
      buffer.append(field, start, length);
    }

    return buffer.toString();
  }
}
