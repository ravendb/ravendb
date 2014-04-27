// -----------------------------------------------------------------------
//  <copyright file="Users_ByName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Indexes
{
	public class Users_ByName : AbstractIndexCreationTask<User>
	{
		public Users_ByName()
		{
			Map = users => from u in users select new { u.Name };
		}
	}
}