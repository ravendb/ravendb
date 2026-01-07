using System;
using System.Diagnostics;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.SchemaValidation;


public class ValidationPath :IDisposable
{
    private readonly AbstractBuffer<int> _sizes;
    private readonly AbstractBuffer<char> _path;
    private string _toString;

    public int Length => _path.Length;

    public ValidationPath()
    {
        _sizes = new RentedBuffer<int>();
        _path = new RentedBuffer<char>();   
    }
    
    public ValidationPath(ByteStringContext allocator)
    {
        _sizes = new ByteStringContextBuffer<int>(allocator);
        _path = new ByteStringContextBuffer<char>(allocator);   
    }
    
    public ValidationPath(JsonOperationContext context)
    {
        _sizes = new JsonOperationContextBuffer<int>(context);
        _path = new JsonOperationContextBuffer<char>(context);   
    }
    
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

    public void Reset()
    {
        _toString = null;
        _sizes.Reset();
        _path.Reset();
    }
    
    public ReadOnlySpan<char> AsSpan() => _path.AsSpan();
    public override string ToString() => _toString ??= _path.ToString();

    public void Dispose()
    {
        _sizes.Dispose();
        _path.Dispose();
    }
}
