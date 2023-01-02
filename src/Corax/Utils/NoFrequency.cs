using System;

namespace Corax.Utils;

public struct NoFrequency : IFrequencyHolder
{
    public void Process(Span<long> matches, int count)
    {
        FrequencyUtils.RemoveFrequencies(matches.Slice(0, count));
    }
}
