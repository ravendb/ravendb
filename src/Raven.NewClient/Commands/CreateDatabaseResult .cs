//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.NewClient.Client.Commands
{
    /// <summary>
    /// The result of a PUT operation
    /// </summary>
    public class CreateDatabaseResult
    {
        /// <summary>
        /// Key of the document that was PUT.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// long? of the document after PUT operation.
        /// </summary>
        public long? ETag { get; set; }
    }
}
