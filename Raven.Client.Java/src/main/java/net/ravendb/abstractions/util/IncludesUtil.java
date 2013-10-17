package net.ravendb.abstractions.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Action2;
import net.ravendb.abstractions.json.JTokenExtensions;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;

import org.apache.commons.lang.StringUtils;


public class IncludesUtil {

  private final static Pattern INCLUDE_PREFIX_REGEX = Pattern.compile("[^(]*(\\([^(]+\\))");

  private static IncludePath getIncludePath(String include) {
    IncludePath result = new IncludePath();
    result.setPath(include);
    Matcher matcher = INCLUDE_PREFIX_REGEX.matcher(include);
    if (matcher.matches() && matcher.groupCount() >= 1) {
      result.setPrefix(matcher.group(1));
      result.setPath(result.getPath().replace(result.getPrefix(), ""));
      result.setPrefix(result.getPrefix().substring(1, result.getPrefix().length() - 1));
    }
    return result;
  }


  private static void executeInternal(RavenJToken token, String prefix, Action2<String, String> loadId) {
    if (token == null) {
      return; // nothing to do
    }

    switch(token.getType()) {
    case ARRAY:
      for (RavenJToken item : (RavenJArray) token) {
        executeInternal(item, prefix, loadId);
      }
      break;
    case STRING:
      loadId.apply(token.value(String.class), prefix);
      break;
    case INTEGER:
      loadId.apply(token.value(Long.class).toString(), prefix);
      break;
      // here we ignore everything else
      // if it ain't a string or array, it is invalid
      // as an id
    }
  }

  private static class IncludePath  {
    private String path;
    private String prefix;

    public String getPath() {
      return path;
    }
    public void setPath(String path) {
      this.path = path;
    }
    public String getPrefix() {
      return prefix;
    }
    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

  }

  public static void include(RavenJObject document, String include, final Action1<String> loadId) {
    if (StringUtils.isEmpty(include) || document == null) {
      return ;
    }

    IncludePath path = getIncludePath(include);

    for (Tuple<RavenJToken, RavenJToken> token : JTokenExtensions.selectTokenWithRavenSyntaxReturningFlatStructure(document, path.getPath())) {
      executeInternal(token.getItem1(), path.getPrefix(), new Action2<String, String>() {

        @Override
        public void apply(String value, String prefix) {
          value = (prefix != null ? prefix + value : value);
          loadId.apply(value);
        }
      });
    }
  }
}
