using System.Collections.Generic;

namespace Raven.Studio.Infrastructure
{
    public class ListContainer<T>
    {
        public IList<T> Items { get; set; }
    }
}