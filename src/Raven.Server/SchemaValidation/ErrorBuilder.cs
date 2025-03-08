using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Server.SchemaValidation;

public interface IErrorBuilder
{
    ValidationPath Path { get; }
    void AddError(DefaultInterpolatedStringHandler message);
}
public class ErrorBuilder : IErrorBuilder
{
    private readonly StringBuilder _errorBuilder = new StringBuilder();

    public ValidationPath Path { get; }

    // ReSharper disable once ConvertConstructorToMemberInitializers
    public ErrorBuilder()
    {
        Path = new ValidationPath();
    }
    
    public void AddError(DefaultInterpolatedStringHandler message) => _errorBuilder.AppendLine(message.ToStringAndClear());

    public string GetErrors() => _errorBuilder.Length != 0 ? _errorBuilder.ToString() : null;
    
    public override string ToString() => _errorBuilder?.ToString();
}
