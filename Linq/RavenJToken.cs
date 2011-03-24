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
        /// Gets the node type for this <see cref="RavenJToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public abstract JTokenType Type { get; }

        /// <summary>
        /// Clones this object
        /// </summary>
        /// <returns>A cloned RavenJToken</returns>
        public abstract RavenJToken CloneToken();
    }
}
