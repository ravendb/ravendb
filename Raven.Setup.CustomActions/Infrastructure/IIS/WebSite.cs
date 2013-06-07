// -----------------------------------------------------------------------
//  <copyright file="WebSite.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Setup.CustomActions.Infrastructure.IIS
{
	public class WebSite
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string PhysicalPath { get; set; }
		public string DefaultAppPool { get; set; }
	}
}