using System;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Server.Threading;

/// <summary>
/// A counter that many threads update and few threads read, each core updates its own cache line.
/// <see cref="Increment"/> returns a <see cref="Decrementor"/> that decrements the same slot, even from another core.
/// </summary>
public sealed class PerCoreCounter
{
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct Slot
    {
        // at the end of the 64 bytes: every indexing reads the array's length (bounds check), which is 8 bytes before
        // the first element, so a value at the start of the first slot would share that line and each write to it
        // would invalidate the line on all the cores
        [FieldOffset(56)]
        public long Value;
    }

    // bounds the memory of a counter on large machines, a few cores sharing a slot still contend far less than all of them
    private const int MaxSlots = 32;

    private readonly Slot[] _slots;
    private readonly int _mask;

    public PerCoreCounter()
    {
        int count = Math.Min(Bits.PowerOf2(Environment.ProcessorCount), MaxSlots);
        _mask = count - 1;
        _slots = new Slot[count];
    }

    public Decrementor Increment()
    {
        int slot = Thread.GetCurrentProcessorId() & _mask;
        Interlocked.Increment(ref _slots[slot].Value);
        return new Decrementor(this, slot);
    }

    private void Decrement(int slot)
    {
        Interlocked.Decrement(ref _slots[slot].Value);
    }

    public long Value
    {
        get
        {
            long sum = 0;
            for (int i = 0; i < _slots.Length; i++)
                sum += Volatile.Read(ref _slots[i].Value);
            return sum;
        }
    }

    public readonly struct Decrementor(PerCoreCounter counter, int slot)
    {
        public void Decrement() => counter.Decrement(slot);
    }
}
