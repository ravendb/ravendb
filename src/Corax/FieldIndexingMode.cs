namespace Corax;


public enum FieldIndexingMode : byte
{
    /// <summary>
    /// Do not index the field value. This field can thus not be searched, but one can still access its contents provided it is stored.
    /// </summary>
    No = 0,
    
    /// <summary>
    /// Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the 
    /// value will be stored as a single term. This is useful for unique Ids like product numbers.
    /// </summary>
    Exact = 1,

    Search = 1 << 2,
    
    /// <summary>
    /// Use the attached analyzer from IndexFieldBinding
    /// </summary>
    Normal = 1 << 3
}
