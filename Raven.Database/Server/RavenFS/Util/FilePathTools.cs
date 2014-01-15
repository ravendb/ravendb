namespace Raven.Database.Server.RavenFS.Util
{
	public static class FilePathTools
	{
		public static string Cannoicalise(string filePath)
		{
			if (!filePath.StartsWith("/"))
				filePath = "/" + filePath;

			return filePath;
		}
	}
}