using Raven.Server.Documents.Indexes;

namespace Corax.Utils;

public sealed class CoraxDynamicItem
{
    public string FieldName;
    public IndexField Field;
    public object Value;
}
