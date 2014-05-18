namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	/// <summary>
	/// Internal MSRDC definitions
	/// </summary>
	public struct Msrdc
	{
		public const uint Version = 0x010000;
		public const uint MinimumCompatibleAppVersion = 0x010000;
		public const uint MinimumDepth = 1;
		public const uint MaximumDepth = 8;
		public const uint MinimumComparebuffer = 100000;
		public const uint MaximumComparebuffer = (1 << 30);
		public const uint DefaultComparebuffer = 3200000;
		public const uint MinimumInputbuffersize = 1024;
		public const uint MinimumHorizonsize = 128;
		public const uint MaximumHorizonsize = 1024 * 16;
		public const uint MinimumHashwindowsize = 2;
		public const uint MaximumHashwindowsize = 96;
		public const uint DefaultHashwindowsize1 = 48;
		public const uint DefaultHorizonsize1 = 1024;
		public const uint DefaultHashwindowsizeN = 2;
		public const uint DefaultHorizonsizeN = 128;
		public const uint MaximumTraitvalue = 63;
		public const uint MinimumMatchesrequired = 1;
		public const uint MaximumMatchesrequired = 16;
	}
}