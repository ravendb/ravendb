namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged
{
	internal enum RdcError : uint
	{
		NoError = 0,
		HeaderVersionNewer,
		HeaderVersionOlder,
		HeaderMissingOrCorrupt,
		HeaderWrongType,
		DataMissingOrCorrupt,
		DataTooManyRecords,
		FileChecksumMismatch,
		ApplicationError,
		Aborted,
		Win32Error
	}
}