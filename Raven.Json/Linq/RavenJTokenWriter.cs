using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace Raven.Json.Linq
{
    public class RavenJTokenWriter : JsonWriter
    {
        private RavenJToken _token;

        /// <summary>
        /// Gets the token being writen.
        /// </summary>
        /// <value>The token being writen.</value>
        public RavenJToken Token
        {
            get
            {
                if (_token != null)
                    return _token;

                return _value;
            }
        }

        /// <summary>
        /// Flushes whatever is in the buffer to the underlying streams and also flushes the underlying stream.
        /// </summary>
        public override void Flush()
        {
        }
    }
}
