//-----------------------------------------------------------------------
// <copyright file="DocumentConventions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Conventions
{
    [Flags]
    public enum IndexAndTransformerReplicationMode
    {
        /// <summary>
        /// No indexes or transformers are updated to replicated instances.
        /// </summary>
        None = 0,

        /// <summary>
        /// All indexes are replicated.
        /// </summary>
        Indexes = 2,

        /// <summary>
        /// All transformers are replicated.
        /// </summary>
        Transformers = 4,
    }
}
