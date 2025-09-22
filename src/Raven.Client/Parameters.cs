using System.Collections.Generic;

namespace Raven.Client
{
    /// <summary>
    /// Represents a collection of query parameters that can be passed to RavenDB queries.
    /// This class extends Dictionary&lt;string, object&gt; to provide a strongly-typed way to manage query parameters.
    /// </summary>
    public sealed class Parameters : Dictionary<string, object>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Parameters"/> class.
        /// </summary>
        public Parameters()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Parameters"/> class with the parameters from another Parameters instance.
        /// </summary>
        /// <param name="other">The Parameters instance to copy from.</param>
        public Parameters(Parameters other) : base(other)
        {

        }
    }
}
