using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Server.SchemaValidation;

public interface IErrorBuilder
{
    void AddError(DefaultInterpolatedStringHandler message);
}
public class ErrorBuilder : IErrorBuilder
{
    private readonly StringBuilder _errorBuilder = new StringBuilder();
    
    public void AddError(DefaultInterpolatedStringHandler message) => _errorBuilder.Append(message.ToStringAndClear());

    public override string ToString() => _errorBuilder.ToString();
}
