//-----------------------------------------------------------------------
// <copyright file="User.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Tests.Bugs
{
	public class User
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string PartnerId { get; set; }
		public string Email { get; set; }
		public string[] Tags { get; set; }
		public int Age { get; set; }

		public bool Active { get; set; }
	}
}