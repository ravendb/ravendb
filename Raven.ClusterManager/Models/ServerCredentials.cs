using System.Net;

namespace Raven.ClusterManager.Models
{
	public class ServerCredentials
	{
		public string Id { get; set; }
		public AuthenticationMode AuthenticationMode { get; set; }
		public string ApiKey { get; set; }
		public string Username { get; set; }
		public string Password { get; set; }
		public string Domain { get; set; }

		public ICredentials GetCredentials()
		{
			if (string.IsNullOrEmpty(Username))
				return null;

			if (string.IsNullOrEmpty(Domain))
				return new NetworkCredential(Username, Password);
			else
				return new NetworkCredential(Username, Password, Domain);
		}
	}
}