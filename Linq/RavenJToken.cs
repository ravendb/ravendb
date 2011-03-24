using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Utilities;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public abstract class RavenJToken
    {
        /// <summary>
        /// Gets the node type for this <see cref="JToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public abstract JTokenType Type { get; }

        internal abstract RavenJToken CloneToken();

        /// <summary>
        /// Creates a new instance of the <see cref="JToken"/>. All child tokens are recursively cloned.
        /// </summary>
        /// <returns>A new instance of the <see cref="JToken"/>.</returns>
        public RavenJToken DeepClone()
        {
            return CloneToken();
        }
    }
}
