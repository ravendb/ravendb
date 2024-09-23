using System.Text;

namespace Raven.Server.SchemaValidation;

internal interface ISchemaRuleValidator<T>
{
    void Validate(T parent, StringBuilder errorBuilder);
}

internal abstract class SchemaRuleValidator<T> : ISchemaRuleValidator<T>
{
    protected string Path { get; }

    // ReSharper disable once ConvertToPrimaryConstructor
    protected SchemaRuleValidator(string path) 
    {
        Path = path;   
    }

    public abstract void Validate(T parent, StringBuilder errorBuilder);
}
