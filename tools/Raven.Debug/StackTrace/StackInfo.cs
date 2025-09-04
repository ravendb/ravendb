using System.Collections.Generic;

namespace Raven.Debug.StackTrace
{
    internal class StackInfo
    {
        public List<int> ThreadIds = new List<int>();
        public List<string> StackTrace = new List<string>();
    }
}