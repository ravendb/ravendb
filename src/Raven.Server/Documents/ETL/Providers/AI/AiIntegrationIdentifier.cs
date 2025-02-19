namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiIntegrationIdentifier(string identifierValue)
{
    public string Value { get; } = identifierValue;

    protected bool Equals(AiIntegrationIdentifier other)
    {
        return Value == other.Value;
    }

    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((AiIntegrationIdentifier)obj);
    }

    public override int GetHashCode()
    {
        return (Value != null ? Value.GetHashCode() : 0);
    }

    public override string ToString()
    {
        return Value;
    }
}
