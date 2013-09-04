//-----------------------------------------------------------------------
// <copyright file="IndexedUser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Tests.Querying
{
	public class IndexedUser
	{
		public int Age { get; set; }
		public DateTime Birthday { get; set; }
		public string Name { get; set; }
		public string Email { get; set; }
	}
}