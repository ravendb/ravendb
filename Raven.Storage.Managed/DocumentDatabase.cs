using System;
using System.Runtime.InteropServices;
using System.Security;

namespace Raven.Storage.Managed
{
	public class DocumentDatabase
	{
		[SuppressUnmanagedCodeSecurity]
		[DllImport("rpcrt4.dll", SetLastError = true)]
		private static extern int UuidCreateSequential(out Guid value);

		public static Guid CreateSequentialUuid()
		{
			Guid value;
			UuidCreateSequential(out value);
			return value;
		}
	}
}