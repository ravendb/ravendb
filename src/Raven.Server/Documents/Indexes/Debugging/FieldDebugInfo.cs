namespace Raven.Server.Documents.Indexes.Debugging;

public record FieldDebugInfo(string Name, IndexFieldType FieldType, IndexedValueType ValueType)
{
    public virtual bool Equals(FieldDebugInfo other)
    {
        return Name.Equals(other.Name);
    }
    
    public override int GetHashCode()
    {
        return Name.GetHashCode();
    }
}
