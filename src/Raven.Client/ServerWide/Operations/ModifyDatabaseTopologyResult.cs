//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.ServerWide.Operations
{
    public class ModifyDatabaseTopologyResult
    {
        /// <summary>
        /// The Raft Command Index that was executed 
        /// </summary>
        public long RaftCommandIndex { get; set; }
    }
}
