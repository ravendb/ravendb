using System.Runtime.Intrinsics;

namespace Voron.Util.PFor;

public readonly struct MaskEntries(uint mask) : ISimdTransform
{
    private readonly Vector256<uint> _mask = Vector256.Create(mask);

    public Vector256<uint> Decode(Vector256<uint> curr, ref Vector256<uint> prev)
    {
        return curr;
    }

    public Vector256<uint> Encode(Vector256<uint> curr, ref Vector256<uint> prev)
    {
        return curr & _mask;
    }
}
