using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raven.Server.SchemaValidation;


public class ValidationPath
{
    //TODO To remove Stack allocation
    private readonly Stack<int> _sizes = new Stack<int>();
    private readonly RentedCharBuffer _path = new RentedCharBuffer();
    private string _toString;

    public int Length => _path.Length;
    
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
        _path.Trim(toRemove);
    }

    public ReadOnlySpan<char> AsSpan() => _path.AsSpan();
    public override string ToString() => _toString ??= _path.ToString();
}
