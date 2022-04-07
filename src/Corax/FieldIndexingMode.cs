namespace Corax;

public enum FieldIndexingMode
{
    /// <summary>
    /// Do not index the field value. This field can thus not be searched, but one can still access its contents provided it is stored.
    /// </summary>
    No,
    
    /// <summary>
    /// Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the 
    /// value will be stored as a single term. This is useful for unique Ids like product numbers.
    /// </summary>
    Exact,

    /// <summary>
    /// Use the attached analyzer from IndexFieldBinding
    /// </summary>
    Normal
}
