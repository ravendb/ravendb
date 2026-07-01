namespace Corax.Querying.Planning;

public sealed class VectorParams
{
    public float MinimumMatch = -1;      // -1 = use index default
    public int NumberOfCandidates = -1;  // -1 = use index default
    public object ResolvedValue;         // the raw vector parameter (string, BlittableJsonReaderArray, etc.)
    public ParamValueType ResolvedValueType;
    public VectorSourceKind Method;      // which embedding method, if any
    public string AiTaskName;            // AI task identifier for embedding.text
}
