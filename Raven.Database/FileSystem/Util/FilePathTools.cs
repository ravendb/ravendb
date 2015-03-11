namespace Raven.Database.FileSystem.Util
{
	public static class FilePathTools
	{
        public static string MakeSureEndsWithSlash(string filePath)
        {
            return filePath.TrimEnd('\\') + "\\";
        }
	}
}