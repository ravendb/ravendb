//-----------------------------------------------------------------------
// <copyright file="Company.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Samples.Includes
{
	public class Company
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string ParentCompanyId { get; set; }
		public string Region { get; set; }
	}
}
