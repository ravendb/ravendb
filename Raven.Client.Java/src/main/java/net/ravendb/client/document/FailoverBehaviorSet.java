package net.ravendb.client.document;

import java.util.Arrays;

import net.ravendb.abstractions.data.EnumSet;

import org.codehaus.jackson.annotate.JsonCreator;



public class FailoverBehaviorSet extends EnumSet<FailoverBehavior, FailoverBehaviorSet> {

  public FailoverBehaviorSet(FailoverBehavior...values) {
    super(FailoverBehavior.class, Arrays.asList(values));
  }

  public FailoverBehaviorSet() {
    super(FailoverBehavior.class);
  }

  public static FailoverBehaviorSet of(FailoverBehavior... values) {
    return new FailoverBehaviorSet(values);
  }

  @JsonCreator
  static FailoverBehaviorSet construct(int value) {
    return construct(new FailoverBehaviorSet(), value);
  }

  @JsonCreator
  static FailoverBehaviorSet construct(String value) {
    return construct(new FailoverBehaviorSet(), value);
  }

}
