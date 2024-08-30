using System.Collections.Generic;

namespace Raven.Server.EventListener;

public class EventComparerByDuration : IComparer<IDurationEvent>
{
    public int Compare(IDurationEvent x, IDurationEvent y)
    {
        if (ReferenceEquals(x, y))
            return 0;
        if (ReferenceEquals(null, y))
            return 1;
        if (ReferenceEquals(null, x))
            return -1;

        return y.DurationInMs.CompareTo(x.DurationInMs);
    }
}
