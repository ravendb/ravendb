using System;
using System.Buffers;
using System.Runtime.InteropServices;
using Nito.Disposables;

namespace Sparrow.Server.Utils;

public static class MemoryMarshalEx
{
    public static ReadOnlyMemory<TTo> Cast<TFrom, TTo>(ReadOnlyMemory<TFrom> memory) where TTo : struct where TFrom : struct
    {
        return new CastedMemoryManager<TFrom, TTo>(memory).Memory;
    }
    
    public class CastedMemoryManager<TFrom, TTo>(ReadOnlyMemory<TFrom> src) : MemoryManager<TTo>
        where TTo : struct
        where TFrom : struct
    {
        protected override void Dispose(bool disposing)
        {    
        }

        public override Span<TTo> GetSpan()
        {
            Memory<TFrom> mem = MemoryMarshal.AsMemory(src);
            return MemoryMarshal.Cast<TFrom,TTo>(mem.Span);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return src.Pin();
        }

        public override void Unpin()
        {
            // nothing to do here
        }
    }
}
