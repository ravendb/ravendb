namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiConnectionStringIdentifier(string identifierValue)
{
    public string Value { get; } = identifierValue;

    public override int GetHashCode()
    {
        return (Value != null ? Value.GetHashCode() : 0);
    }

    protected bool Equals(AiConnectionStringIdentifier other)
    {
        return Value == other.Value;
    }

    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((AiConnectionStringIdentifier)obj);
    }

    public override string ToString()
    {
        return Value;
    }
}
