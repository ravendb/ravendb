namespace Corax.Querying.Planning;

/// <summary>Where the vector data for a vector.search() query comes from.</summary>
public enum VectorSourceKind : byte
{
    Inline,         // direct value or embedding.forRaw — vector data provided in the query
    FromDocument,   // embedding.forDoc(docId) — vector copied from another document
    FromText,       // embedding.text(text, ai.task(task)) — server generates embedding from text
}
