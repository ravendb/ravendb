package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.data.Constants;

import org.apache.commons.lang.StringUtils;


/**
 * Helper function for numeric to indexed string and vice versa
 */
public class NumberUtil {
  /**
   * Translate a number to an indexable string
   * @param number
   * @return
   */
  public static String numberToString(int number) {
    return "Ix" + number;
  }


  /**
   * Translate a number to an indexable string
   * @param number
   * @return
   */
  public static String numberToString(long number) {
    return "Lx" + number;
  }

  /**
   * Translate a number to an indexable string
   * @param number
   * @return
   */
  public static String numberToString(float number) {
    return "Fx" + trimZeros(String.format(Constants.getDefaultLocale(), "%.11f", number));
  }

  /**
   * Translate a number to an indexable string
   * @param number
   * @return
   */
  public static String numberToString(double number) {
    return "Dx" + trimZeros(String.format(Constants.getDefaultLocale(), "%.11f", number));
  }

  /**
   * Removes zeros from the end of the string but only from fractional part
   * if string ends with dot after first operation, we remove dot as well.
   * @param input
   * @return
   */
  public static String trimZeros(String input) {
    if (!input.contains(".")) {
      return input;
    }
    input = StringUtils.stripEnd(input, "0");
    return StringUtils.stripEnd(input, ".");
  }

  /**
   * Translate an indexable string to a number
   * @param number
   * @return
   */
  public static Object stringToNumber(String number)
  {
    if (number == null)
      return null;

    if ("NULL".equalsIgnoreCase(number) || "*".equalsIgnoreCase(number)) {
      return null;
    }
    if(number.length() <= 2) {
      throw new IllegalArgumentException("String must be greater than 2 characters");
    }
    String num = number.substring(2);
    String prefix = number.substring(0, 2);
    switch (prefix) {
    case "0x":
      switch (num.length()) {
      case 8:
        return Integer.parseInt(num, 16);
      case 16:
        return Long.parseLong(num, 16);
      }
      break;
    case "Ix":
      return Integer.parseInt(num);
    case "Lx":
      return Long.parseLong(num);
    case "Fx":
      return Float.parseFloat(num);
    case "Dx":
      return Double.parseDouble(num);
    }

    throw new IllegalArgumentException(String.format("Could not understand how to parse: '%d'", number));

  }
}
