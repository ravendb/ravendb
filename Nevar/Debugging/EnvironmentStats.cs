using System.Collections.Generic;
using Nevar.Impl;

namespace Nevar.Debugging
{
    public class EnvironmentStats
    {
        public long FreePages;
        public long FreePagesOverhead;
        public long RootPages;
        public long HeaderPages;
        public long UnallocatedPagesAtEndOfFile;
    }
}