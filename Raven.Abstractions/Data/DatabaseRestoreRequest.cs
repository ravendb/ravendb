//-----------------------------------------------------------------------
// <copyright file="DatabaseRestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
    public class DatabaseRestoreRequest : AbstractRestoreRequest
	{
		public string DatabaseName { get; set; }

        public string DatabaseLocation { get; set; }

		public bool DisableReplicationDestinations { get; set; }

		public bool GenerateNewDatabaseId { get; set; }
	}
}