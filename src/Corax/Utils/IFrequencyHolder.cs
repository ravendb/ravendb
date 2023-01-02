using System;

namespace Corax.Utils;

public interface IFrequencyHolder
{
    void Process(Span<long> matches, int count);
}
