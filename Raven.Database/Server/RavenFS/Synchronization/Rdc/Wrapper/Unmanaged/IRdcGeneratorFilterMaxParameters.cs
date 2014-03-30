using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A72-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	internal interface IRdcGeneratorFilterMaxParameters
	{
		Int32 GetHorizonSize(out uint horizonSize);

		Int32 SetHorizonSize(uint horizonSize);

		Int32 GetHashWindowSize(out uint hashWindowSize);

		Int32 SetHashWindowSize(uint hashWindowSize);
	}
}