package net.ravendb.abstractions.json;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;


public class JTokenExtensions {

  public static Iterable<Tuple<RavenJToken, RavenJToken>> selectTokenWithRavenSyntaxReturningFlatStructure(RavenJToken self, String path) {
    List<Tuple<RavenJToken, RavenJToken>> resultList = new ArrayList<>();

    String[] pathParts = path.split(",");
    List<String> notEmptyParts = new ArrayList<>();
    for (String pathPart: pathParts) {
      if (StringUtils.isNotEmpty(pathPart)) {
        notEmptyParts.add(pathPart);
      }
    }
    pathParts = notEmptyParts.toArray(new String[0]);
    RavenJToken result = self.selectToken(pathParts[0]);
    if (pathParts.length == 1) {
      resultList.add(new Tuple<>(result, self));
      return resultList;
    }

    if (result == null || result.getType() == JTokenType.NULL) {
      return resultList;
    }

    if (result.getType() == JTokenType.OBJECT) {
      RavenJObject ravenJObject = (RavenJObject) result;
      if (ravenJObject.containsKey("$values")){
        result = ravenJObject.value(RavenJToken.class, "$values");
      }
    }

    for (RavenJToken item: result.values(RavenJToken.class)) {
      for (Tuple<RavenJToken, RavenJToken> subItem : JTokenExtensions.selectTokenWithRavenSyntaxReturningFlatStructure(item, StringUtils.join(ArrayUtils.subarray(pathParts, 1, pathParts.length), ","))) {
        resultList.add(subItem);
      }
    }
    return resultList;
  }

}
