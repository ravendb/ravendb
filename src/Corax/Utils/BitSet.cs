using System;
using System.Buffers;
namespace Corax.Utils;

public struct BitSet : IDisposable
{
    public ulong[] Bits;
    private readonly int _amount;
    private readonly ulong _lastElementMask;
    public BitSet(int countOfBits)
    {
        _amount = (countOfBits / 64 + (countOfBits % 64 == 0 ? 0 : 1));
        Bits = ArrayPool<ulong>.Shared.Rent(_amount);
        new Span<ulong>(Bits).Clear();

        _lastElementMask = 0;
        for (int i = 0; i < countOfBits % 64; ++i)
            _lastElementMask |= 1UL << i;
    }
    
    public void Set(int index)
    {
        Bits[index / 64] |= 1UL << index % 64;
    }
    
    public void Clear()
    {
        new Span<ulong>(Bits).Clear();
    }

    public bool IsAllSet()
    {
        for (int i = 0; i < _amount; ++i)
        {
            ulong bitmap = Bits[i];
            if (i + 1 != _amount)
            {
                if ((bitmap ^ ulong.MaxValue) != 0)
                    return false;
            }
            else
            {
                if ((bitmap ^ _lastElementMask) != 0)
                    return false;
            }
        }

        return true;
    }

    public void Dispose()
    {
        ArrayPool<ulong>.Shared.Return(Bits);
    }
}
