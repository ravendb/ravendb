using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A71-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	public interface IRdcGeneratorParameters
	{
		Int32 GetGeneratorParametersType([Out] out GeneratorParametersType parametersType);

		Int32 GetParametersVersion([Out] out uint currentVersion, [Out] out uint minimumCompatabileAppVersion);

		Int32 GetSerializeSize([Out] out uint size);

		Int32 Serialize(uint size, [Out] out IntPtr parametersBlob, [Out] out uint bytesWritten);
	}
}