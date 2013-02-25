namespace Raven.Abstractions.Indexing
{
    /// <summary>
    /// Specifies whether to include term vectors for a field
    /// </summary>
    public enum FieldTermVector
    {
        /// <summary>
        /// Do not store term vectors
        /// </summary>
        No,
        /// <summary>
        /// Store the term vectors of each document. A term vector is a list of the document's
        /// terms and their number of occurrences in that document.
        /// </summary>
        Yes,        
        /// <summary>
        /// Store the term vector + token position information
        /// </summary>
        WithPositions,
        /// <summary>
        /// Store the term vector + Token offset information
        /// </summary>
        WithOffsets,
        /// <summary>
        /// Store the term vector + Token position and offset information
        /// </summary>
        WithPositionsAndOffsets
    }
}
