package net.ravendb.abstractions.data;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ravendb.abstractions.basic.Reference;

import org.apache.commons.lang.StringUtils;


public final class HighlightedField {

  private static final Pattern pattern = Pattern.compile("^(\\w+):(\\d+),(\\d+)(,(\\w+))?$");

  private String field;
  private int fragmentLength;
  private int fragmentCount;
  private String fragmentsField;


  public String getField() {
    return field;
  }


  public int getFragmentLength() {
    return fragmentLength;
  }


  public int getFragmentCount() {
    return fragmentCount;
  }


  public String getFragmentsField() {
    return fragmentsField;
  }


  public HighlightedField(String field, int fragmentLength, int fragmentCount, String fragmentsField) {
    this.field = field;
    this.fragmentLength = fragmentLength;
    this.fragmentCount = fragmentCount;
    this.fragmentsField = fragmentsField;
  }


  public static boolean TryParse(String value, Reference<HighlightedField> result)
  {
    result.value = null;

    Matcher matcher = pattern.matcher(value);
    if (!matcher.matches()) {
      return false;
    }

    String field = matcher.group(1);
    if (StringUtils.isBlank(field)) {
      return false;
    }


    Integer fragmentLength;
    try {
      fragmentLength = Integer.valueOf(matcher.group(2));
    } catch (NumberFormatException e) {
      return false;
    }

    Integer fragmentCount;
    try {
      fragmentCount = Integer.valueOf(matcher.group(3));
    } catch (NumberFormatException e) {
      return false;
    }

    String fragmentsField = matcher.group(5);
    result.value = new HighlightedField(field, fragmentLength, fragmentCount, fragmentsField);

    return true;
  }

  @Override
  public String toString() {
    String frag = StringUtils.isEmpty(this.fragmentsField) ? "" : "," + this.fragmentsField;
    return String.format("%s:%d,%d%s", this.field, this.fragmentLength, this.fragmentCount, frag);
  }

  @Override
  public HighlightedField clone() {
    return new HighlightedField(field, fragmentLength, fragmentCount, fragmentsField);
  }

}
