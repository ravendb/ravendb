namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper
{
	public class RdcVersion
	{
		public uint CurrentVersion { get; set; }
		public uint MinimumCompatibleAppVersion { get; set; }
	}
}