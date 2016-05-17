using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Static
{
    public abstract class StaticIndexBase
    {
        protected StaticIndexBase()
        {
            ForCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public readonly HashSet<string> ForCollections;
    }
}