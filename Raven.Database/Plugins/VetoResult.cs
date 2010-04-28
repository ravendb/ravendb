namespace Raven.Database.Plugins
{
	public class VetoResult
	{
		public static VetoResult Allowed
		{
			get { return new VetoResult(true, "allowed"); }
		}

		public static VetoResult Deny(string reason)
		{
			return new VetoResult(false, reason);
		}

		private VetoResult(bool allowed, string reason)
		{
			IsAllowed = allowed;
			Reason = reason;
		}

		public bool IsAllowed { get; private set; }
		public string Reason { get; private set; }
	}
}