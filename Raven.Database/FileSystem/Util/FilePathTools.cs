using Raven.Database.Extensions;

namespace Raven.Database.FileSystem.Util
{
	public static class FilePathTools
	{
		public static string MakeSureEndsWithSlash(string filePath)
		{
			return filePath.TrimEnd('\\') + "\\";
		}

		public static string ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(string workingDirectory, string path)
		{
			if (string.IsNullOrEmpty(workingDirectory) || string.IsNullOrEmpty(path))
				return path;

			if (path.StartsWith(@"~/") || path.StartsWith(@"~\"))
				path = path
					.Replace(@"~/", workingDirectory)
					.Replace(@"~\", workingDirectory);

			return MakeSureEndsWithSlash(path.ToFullPath());
		}
	}
}