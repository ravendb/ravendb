using System;
using System.Diagnostics;

namespace Raven.Server.SchemaValidation;


public class ValidationPath
{
    //TODO To remove Stack allocation
    private readonly RentedBuffer<int> _sizes = new RentedBuffer<int>();
    private readonly RentedCharBuffer _path = new RentedCharBuffer();
    private string _toString;

    public int Length => _path.Length;
    
    public void StepIn(string property)
    {
        _toString = null;
        var before = _path.Length;
        if (_path.Length != 0)
            _path.Append('.');
        
        _path.Append(property);
        _sizes.Append(_path.Length - before);
    }
    public void StepIn(int index)
    {
        _toString = null;
        Debug.Assert(_path.Length != 0);
        var current = $"[{index}]";
        _sizes.Append(current.Length);
        
        _path.Append(current);
    }
    
    public void StepOut()
    {
        _toString = null;
        var toRemove = _sizes.AsSpan()[_sizes.Length - 1];
        _sizes.Trim(1);
        _path.Trim(toRemove);
    }

    public ReadOnlySpan<char> AsSpan() => _path.AsSpan();
    public override string ToString() => _toString ??= _path.ToString();
}
