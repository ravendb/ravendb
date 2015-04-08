using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Indexing
{
    public class DocCountWithSampleDocIds
    {
        public int Count { get; set; }
        public HashSet<string> SampleDocsIds { get; set; }
    }
}
