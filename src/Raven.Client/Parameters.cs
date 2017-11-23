using System.Collections.Generic;

namespace Raven.Client
{
    public class Parameters : Dictionary<string, object>
    {
        public Parameters()
        {
        }

        public Parameters(IDictionary<string, object> dictionary) : base(dictionary)
        {
        }

        public Parameters(IDictionary<string, object> dictionary, IEqualityComparer<string> comparer) : base(dictionary, comparer)
        {
        }
    }
}
