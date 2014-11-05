using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A80-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	internal interface IRdcSimilarityGenerator
	{
		Int32 EnableSimilarity();

		Int32 Results(out SimilarityData similarityData);
	}
}