using System.Collections.Generic;

namespace Raven.Client
{
    public class Parameters : Dictionary<string, object>
    {
        public Parameters()
        {

        }

        public Parameters(Parameters other) : base(other)
        {

        }
    }
}
