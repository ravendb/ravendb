//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.ServerWide.Operations
{
    public class DeleteDatabaseResult
    {
        public long RaftCommandIndex { get; set; }
        public string[] PendingDeletes { get; set; }
    }
}
