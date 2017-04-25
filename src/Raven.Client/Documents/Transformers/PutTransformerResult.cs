//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Transformers
{
    public class PutTransformerResult
    {
        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public string Transformer { get; set; }

        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public int Etag { get; set; }
    }
}
