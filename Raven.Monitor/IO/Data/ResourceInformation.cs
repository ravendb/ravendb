// -----------------------------------------------------------------------
//  <copyright file="ResourceInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Monitor.IO.Data
{
	internal class ResourceInformation
	{
		public string ResourceName { get; set; }

		public ResourceType ResourceType { get; set; }

		public PathType PathType { get; set; }
	}
}