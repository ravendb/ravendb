using System.Collections.Generic;

namespace Raven.Munin
{
    public interface ICompererAndEquality<in TKey> : IComparer<TKey>, IEqualityComparer<TKey>
    {
        
    }
}