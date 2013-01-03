using System;
using Windows.Storage;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public class IsolatedStorageFile
	{
		public static IsolatedStorageFile GetUserStoreForSite()
		{
			throw new NotImplementedException();
		}
	}
}