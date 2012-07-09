namespace Raven.Bundles.Authentication
{
	public class UserDatabaseAccess
	{
		public bool ReadOnly { get; set; }
		public bool Admin { get; set; }
		public string Name { get; set; }
	}
}