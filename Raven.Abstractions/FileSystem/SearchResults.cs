using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{
    public class SearchResults
    {
        public List<FileHeader> Files { get; set; }
        public int FileCount { get; set; }
        public int Start { get; set; }
        public int PageSize { get; set; }
    }
}
