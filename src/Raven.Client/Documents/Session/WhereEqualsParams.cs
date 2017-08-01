namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Parameters for the Where Equals call
    /// </summary>
    public class WhereParams
    {
        /// <summary>
        /// Create a new instance 
        /// </summary>
        public WhereParams()
        {
            IsNestedPath = false;
            AllowWildcards = false;
        }

        /// <summary>
        /// The field name
        /// </summary>
        public string FieldName { get; set; }

        /// <summary>
        /// The field value
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// Should the field allow wildcards
        /// </summary>
        public bool AllowWildcards { get; set; }

        /// <summary>
        /// Is this a root property or not?
        /// </summary>
        public bool IsNestedPath { get; set; }
        
        public bool Exact { get; set; }
    }
}
