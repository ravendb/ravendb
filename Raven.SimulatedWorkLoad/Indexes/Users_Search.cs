// -----------------------------------------------------------------------
//  <copyright file="Users_Search.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.SimulatedWorkLoad.Model;

namespace Raven.SimulatedWorkLoad.Indexes
{
	public class Users_Search : AbstractIndexCreationTask<User>
	{
		public Users_Search()
		{
			Map = users =>
				  from user in users
				  select new
				  {
					  Query = new object[] { user.First, user.Last, user.Email, user.Email.Split('@'), user.Phone }
				  };
		}
	}
}