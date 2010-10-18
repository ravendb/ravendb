using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Storage.Managed.Impl
{
    public class RecordingComparer : IComparer<JToken>
    {
        public JToken LastComparedTo { get; set; }

        public int Compare(JToken x, JToken y)
        {
            LastComparedTo = x;
            return JTokenComparer.Instance.Compare(x, y);
        }
    }
}