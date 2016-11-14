using System.Collections.Generic;
using System.Linq;

namespace Raven.NewClient.Abstractions.Extensions 
{
    public static class SetExtensions
    {
        public static bool ContentEquals<TKey>(ISet<TKey> x, ISet<TKey> y)
        {
            if (x.Count != y.Count)
                return false;
            return x.All(y.Contains);
        } 
    }
}
