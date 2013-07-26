package raven.linq.dsl.visitors;

import java.util.HashSet;
import java.util.Set;

public class RootsExtractorContext {
  private Set<String> roots = new HashSet<>();
  private Set<String> introducedInLambda = new HashSet<>();

  public Set<String> getRoots() {
    return roots;
  }
  public void setRoots(Set<String> roots) {
    this.roots = roots;
  }
  public Set<String> getIntroducedInLambda() {
    return introducedInLambda;
  }
  public void setIntroducedInLambda(Set<String> introducedInLambda) {
    this.introducedInLambda = introducedInLambda;
  }
  public void addRoot(String name) {
    roots.add(name);
  }

  public void addLambda(String lambdaName) {
    introducedInLambda.add(lambdaName);
  }

}
