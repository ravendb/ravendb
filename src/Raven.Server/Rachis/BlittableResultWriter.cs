using System;
using Sparrow.Threading;

namespace Raven.Server.Rachis;

public class BlittableResultWriter(Func<object, object> writeResultFunc) : IDisposable
{
    private readonly SingleUseFlag _invalid = new SingleUseFlag();

    public object Result { private set; get; }

    public void CopyResult(object result)
    {
        lock (this)
        {
            Result =  _invalid.IsRaised() 
                ? null : 
                writeResultFunc.Invoke(result);
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
