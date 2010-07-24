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
	}
}