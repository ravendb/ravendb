using System;
using Sparrow.Threading;

namespace Raven.Server.Rachis;

public class BlittableResultWriter : IDisposable
{
    private readonly SingleUseFlag _invalid = new SingleUseFlag();
    private readonly Func<object, object> _writeResultFunc;

    public BlittableResultWriter(Func<object, object> writeResultFunc)
    {
        _writeResultFunc = writeResultFunc;
    }

    public object Result { private set; get; }

    public void CopyResult(object result)
    {
        lock (this)
        {
            Result =  _invalid.IsRaised() 
                ? null : 
                _writeResultFunc.Invoke(result);
        }
    }

    public void InvalidateWriter()
    {
        lock (this)
        {
            _invalid.Raise();
        }
    }

    public void Dispose() => InvalidateWriter();
}
