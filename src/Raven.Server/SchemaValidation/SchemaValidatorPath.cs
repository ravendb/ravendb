using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Raven.Server.SchemaValidation;

//TODO To optimize
public class SchemaValidatorPath
{
    private readonly Stack<int> _sizes = new Stack<int>();
    private readonly StringBuilder _path = new StringBuilder();
    private string _toString;

    public void StepIn(string property)
    {
        _toString = null;
        if (_path.Length != 0)
            property = "." + property;
        
        _sizes.Push(property.Length);
        _path.Append(property);
    }
    public void StepIn(int index)
    {
        _toString = null;
        Debug.Assert(_path.Length != 0);
        var current = $"[{index}]";
        _sizes.Push(current.Length);
        
        _path.Append(current);
    }
    

    public void StepOut()
    {
        _toString = null;
        var toRemove = _sizes.Pop();
        _path.Remove(_path.Length - toRemove, toRemove);
    }

    public override string ToString() => _toString ?? _path.ToString();
}
