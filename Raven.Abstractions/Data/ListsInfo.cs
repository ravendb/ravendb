using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Data
{
    public class ListsInfo
    {
        public string Name { get; set; }
        public long Count { get; set; }
        public long SizeOnDiskInBytes { get; set; }
        public long MinListItemSizeOnDiskInBytes { get; set; }
        public long MaxListItemSizeOnDiskInBytes { get; set; }
        public long AverageListItemSizeOnDiskInBytes { get; set; }
    }
}
