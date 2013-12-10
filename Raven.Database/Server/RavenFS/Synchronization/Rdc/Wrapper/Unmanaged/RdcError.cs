namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	public enum RdcError : uint
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