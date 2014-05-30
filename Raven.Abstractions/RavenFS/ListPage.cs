using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.RavenFS
{
    public class ListPage<T>
    {
        public ListPage(IEnumerable<T> items, int total)
        {
            TotalCount = total;
            Items = items.ToList();
        }

        public int TotalCount { get; set; }
        public IList<T> Items { get; set; }
    }
}
