package raven.abstractions.data;

import raven.abstractions.basic.UseSharpEnum;

@UseSharpEnum
//TODO: serialize enums as ints!
public enum FacetTermSortMode {
  VALUE_ASC,
  VALUE_DESC,
  HITS_ASC,
  HITS_DESC;
}
