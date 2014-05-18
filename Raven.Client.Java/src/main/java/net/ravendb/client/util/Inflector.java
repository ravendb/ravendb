package net.ravendb.client.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * Class responsible for pluralizing words
 *
 */
public class Inflector {

  private static final List<Rule> plurals = new ArrayList<>();
  private static final List<Rule> singulars = new ArrayList<>();
  private static final List<String> uncountables = new ArrayList<>();

  private Inflector() {
    // empty by design
  }

  static {
    addPlural("(.*)$", "$1s");
    addPlural("(.*)s$", "$1s");
    addPlural("(ax|test)is$", "$1es");
    addPlural("(octop|vir)us$", "$1i");
    addPlural("(alias|status)$", "$1es");
    addPlural("(bu)s$", "$1ses");
    addPlural("(buffal|tomat)o$", "$1oes");
    addPlural("([ti])um$", "$1a");
    addPlural("(.*)sis$", "$1ses");
    addPlural("(?:([^f])fe|([lr])f)$", "$1$2ves");
    addPlural("(hive)$", "$1s");
    addPlural("([^aeiouy]|qu)y$", "$1ies");
    addPlural("(x|ch|ss|sh)$", "$1es");
    addPlural("(matr|vert|ind)ix|ex$", "$1ices");
    addPlural("([m|l])ouse$", "$1ice");
    addPlural("^(ox)$", "$1en");
    addPlural("(quiz)$", "$1zes");

    addSingular("(.*)s$", "");
    addSingular("(n)ews$", "$1ews");
    addSingular("([ti])a$", "$1um");
    addSingular("((a)naly|(b)a|(d)iagno|(p)arenthe|(p)rogno|(s)ynop|(t)he)ses$", "$1$2sis");
    addSingular("(^analy)ses$", "$1sis");
    addSingular("([^f])ves$", "$1fe");
    addSingular("(hive)s$", "$1");
    addSingular("(tive)s$", "$1");
    addSingular("([lr])ves$", "$1f");
    addSingular("([^aeiouy]|qu)ies$", "$1y");
    addSingular("(s)eries$", "$1eries");
    addSingular("(m)ovies$", "$1ovie");
    addSingular("(x|ch|ss|sh)es$", "$1");
    addSingular("([m|l])ice$", "$1ouse");
    addSingular("(bus)es$", "$1");
    addSingular("(o)es$", "$1");
    addSingular("(shoe)s$", "$1");
    addSingular("(cris|ax|test)es$", "$1is");
    addSingular("(octop|vir)i$", "$1us");
    addSingular("(alias|status)es$", "$1");
    addSingular("^(ox)en", "$1");
    addSingular("(vert|ind)ices$", "$1ex");
    addSingular("(matr)ices$", "$1ix");
    addSingular("(quiz)zes$", "$1");

    addIrregular("person", "people");
    addIrregular("man", "men");
    addIrregular("child", "children");
    addIrregular("sex", "sexes");
    addIrregular("move", "moves");

    addUncountable("equipment");
    addUncountable("information");
    addUncountable("rice");
    addUncountable("money");
    addUncountable("species");
    addUncountable("series");
    addUncountable("fish");
    addUncountable("sheep");
  }

  private static class Rule {
    private final Pattern regex;
    private final String replacement;

    public Rule(String pattern, String replacement) {
      regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
      this.replacement = replacement;
    }

    public String apply(String word) {
      Matcher matcher = regex.matcher(word);
      boolean find = matcher.find();
      if (!find) {
        return null;
      }
      return matcher.replaceFirst(replacement);
    }
  }


  public static String pluralize(String word) {
    return applyRules(plurals, word);
  }

  public static String singularize(String word) {
    return applyRules(singulars, word);
  }

  public static String capitalize(String word) {
    return StringUtils.capitalize(word);
  }

  private static void addIrregular(String singular, String plural) {
    addPlural("(" + singular.charAt(0) + ")" + singular.substring(1) + "$", "$1" + plural.substring(1));
    addSingular("(" + plural.charAt(0) + ")" + plural.substring(1) + "$", "$1" + singular.substring(1));
  }

  private static void addUncountable(String word) {
    uncountables.add(word.toLowerCase());
  }

  private static void addPlural(String rule, String replacement) {
    plurals.add(new Rule(rule, replacement));
  }

  private static void addSingular(String rule, String replacement) {
    singulars.add(new Rule(rule, replacement));
  }

  private static String applyRules(List<Rule> rules, String word) {
    String result = word;

    if (!uncountables.contains(word.toLowerCase())) {
      for (int i = rules.size() - 1; i >= 0; i--) {
        Rule rule = rules.get(i);

        if ((result = rule.apply(word)) != null)
        {
          break;
        }
      }
    }

    return result;
  }

}
