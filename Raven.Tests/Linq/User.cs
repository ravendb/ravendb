//-----------------------------------------------------------------------
// <copyright file="User.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Tests.Linq
{
	public class User
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public int Age { get; set; }
		public string Info { get; set; }
		public bool Active { get; set; }
		public DateTime Created { get; set; }

		public User()
		{
			Name = String.Empty;
			Age = default(int);
			Info = String.Empty;
			Active = false;
		}        
	}
}