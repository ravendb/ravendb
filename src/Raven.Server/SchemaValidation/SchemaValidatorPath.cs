using System.Collections.Generic;

namespace Raven.Server.SchemaValidation;

public class SchemaValidatorPath
{
    private readonly Stack<string> _path = new Stack<string>();
    private string _toString;

    public void StepIn(string property)
    {
        _toString = null;
        _path.Push(property);
    }

    public void StepOut()
    {
        _toString = null;
        _path.Pop();
    }

    public override string ToString()
    {
        _toString ??= string.Join(".", _path);
        return _toString;
    }
}
