//-----------------------------------------------------------------------
// <copyright file="AuthorizationRole.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Bundles.Authorization.Model
{
	public class AuthorizationRole
	{
		public string Id { get; set; }
		public List<OperationPermission> Permissions { get; set; }

		public AuthorizationRole()
		{
			Permissions = new List<OperationPermission>();
		}

		public bool DeepEquals(AuthorizationRole other)
		{
			if(other == null)
				return false;

			if (Permissions.Count != other.Permissions.Count)
				return false;

			for (int i = 0; i < Permissions.Count; i++)
			{
				if (Permissions[i].DeepEquals(other.Permissions[i]) == false)
				{
					return false;
				}
			}

			return true;
		}
	}
}
