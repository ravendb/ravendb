package net.ravendb.abstractions.util;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Base62Util {

  public static String base62Random() {
    return base62ToString(Math.abs(ThreadLocalRandom.current().nextLong()));
  }

  private static String base62ToString(long value) {
    // Divides the number by 64, so how many 64s are in
    // 'value'. This number is stored in Y.
    // e.g #1
    // 1) 1000 / 62 = 16, plus 8 remainder (stored in x).
    // 2) 16 / 62 = 0, remainder 16
    // 3) 16, 8 or G8:
    // 4) 65 is A, add 6 to this = 71 or G.
    //
    // e.g #2:
    // 1) 10000 / 62 = 161, remainder 18
    // 2) 161 / 62 = 2, remainder 37
    // 3) 2 / 62 = 0, remainder 2
    // 4) 2, 37, 18, or 2,b,I:
    // 5) 65 is A, add 27 to this (minus 10 from 37 as these are digits) = 92.
    // Add 6 to 92, as 91-96 are symbols. 98 is b.
    // 6)
    long x = value % 62L;
    long y = value / 62L;
    if (y > 0) {
      return base62ToString(y) + valToChar(x);
    }
    return new Character(valToChar(x)).toString();
  }

  private static char valToChar(long value) {
    if (value > 9) {
      int ascii = (65 + ((int)value - 10));
      if (ascii > 90)
        ascii += 6;
      return (char)ascii;
    } else {
      return String.valueOf(value).charAt(0);
    }
  }

  public static String toBase62(UUID uuid) {
    return  base62ToString(uuid.getLeastSignificantBits()) + base62ToString(uuid.getMostSignificantBits());
  }

}
