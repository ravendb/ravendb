namespace Raven.Database.Plugins
{
	public class VetoResult
	{
		public VetoResult(bool allowed, string reason)
		{
			Allowed = allowed;
			Reason = reason;
		}

		public bool Allowed { get; private set; }
		public string Reason { get; private set; }
	}
}