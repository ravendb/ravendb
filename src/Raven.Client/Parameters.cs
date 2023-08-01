using System.Collections.Generic;

namespace Raven.Client
{
    public sealed class Parameters : Dictionary<string, object>
    {
        public Parameters()
        {

        }

        public Parameters(Parameters other) : base(other)
        {

        }
    }
}
