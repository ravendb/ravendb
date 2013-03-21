//-----------------------------------------------------------------------
// <copyright file="AuthorizationUser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Bundles.Authorization.Model
{
	public class AuthorizationUser
	{
		public string Name { get; set; }
		public string Id { get; set; }
		public List<string> Roles { get; set; }
		public List<OperationPermission> Permissions { get; set; }

		public AuthorizationUser()
		{
			Roles = new List<string>();
			Permissions = new List<OperationPermission>();
		}
	}
}
