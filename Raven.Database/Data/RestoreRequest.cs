//-----------------------------------------------------------------------
// <copyright file="RestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Data
{
	public class RestoreRequest
	{
		public string RestoreLocation { get; set; }
		public string DatabaseLocation { get; set; }
	}
}
