package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.basic.UseSharpEnum;

/**
 *  Options for indexing a field
 */
@UseSharpEnum
public enum FieldIndexing {
  /**
   * Do not index the field value. This field can thus not be searched, but one can still access its contents provided it is stored.
   */
  NO,
  /**
   * Index the tokens produced by running the field's value through an Analyzer. This is useful for common text.
   */
  ANALYZED,

  /**
   * Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the
   * value will be stored as a single term. This is useful for unique Ids like product numbers.
   */
  NOT_ANALYZED,

  /**
   *  Index this field using the default internal analyzer: LowerCaseKeywordAnalyzer
   */
  DEFAULT;
}
