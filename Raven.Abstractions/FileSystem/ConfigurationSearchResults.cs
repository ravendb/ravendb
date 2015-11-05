using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{

    public class ConfigurationSearchResults
    {
        public IList<string> ConfigNames { get; set; }
        public int TotalCount { get; set; }
        public int Start { get; set; }
        public int PageSize { get; set; }
    }
}
