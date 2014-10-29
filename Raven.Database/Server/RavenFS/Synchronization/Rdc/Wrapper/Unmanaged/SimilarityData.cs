using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[StructLayout(LayoutKind.Sequential, Pack = 4)]
	public struct SimilarityData
	{
		public char[] Data; // m_Data[16]
	}
}