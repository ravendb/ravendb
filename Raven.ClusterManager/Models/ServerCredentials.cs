namespace Raven.ClusterManager.Models
{
	public class ServerCredentials
	{
		public AuthenticationMode AuthenticationMode { get; set; }
		public string ApiKey { get; set; }
		public string Username { get; set; }
		public string Password { get; set; }
		public string Domain { get; set; }
	}
}