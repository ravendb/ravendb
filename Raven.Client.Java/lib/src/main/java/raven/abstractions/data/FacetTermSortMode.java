package raven.abstractions.data;

import raven.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum FacetTermSortMode {
  VALUE_ASC,
  VALUE_DESC,
  HITS_ASC,
  HITS_DESC;
}
