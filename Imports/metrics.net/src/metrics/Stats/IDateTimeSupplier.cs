using System;

namespace metrics.Stats
{
    public interface IDateTimeSupplier
    {
        DateTime UtcNow { get; }
    }
}