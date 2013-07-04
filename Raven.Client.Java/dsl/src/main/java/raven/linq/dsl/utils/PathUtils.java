package raven.linq.dsl.utils;

import com.mysema.query.types.Path;

public class PathUtils {

  /**
   * @param selector
   * @return if path has format: root.property
   */
  public static boolean checkForPathWithSingleGetter(Path<?> selector) {
    if (selector.getMetadata().getRoot() == null) {
      return false;
    }
    if (!selector.getMetadata().getParent().equals(selector.getMetadata().getRoot())) {
      return false;
    }
    return true;
  }
}
