package raven.client.util;

/**
 * Class responsible for pluralizing words
 *
 */
public class Inflector {
  private Inflector() {
    // empty by design
  }
  public static String pluralize(String word) {
    if (word.equals("Company")) {
      return "Companies";
    }
    //TODO: rewrite real imp
    return word + "s";
  }

}
