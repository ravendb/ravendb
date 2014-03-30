using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Raven.Database.Server.RavenFS.Extensions
{
	public static class IOExtensions
	{
		public static void CopyDirectory(string from, string to, string[] skip)
		{
			try
			{
				CopyDirectory(new DirectoryInfo(from), new DirectoryInfo(to), skip);
			}
			catch (Exception e)
			{
				throw new Exception(String.Format("Exception encountered copying directory from {0} to {1}.", from, to), e);
			}
		}

		static void CopyDirectory(DirectoryInfo source, DirectoryInfo target, string[] skip)
		{
			if (!target.Exists)
				Directory.CreateDirectory(target.FullName);

			// copy all files in the immediate directly
			foreach (FileInfo fi in source.GetFiles())
			{
				fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);
			}

			// and recurse
			foreach (var diSourceDir in source.GetDirectories())
			{
				if (skip.Contains(diSourceDir.Name))
					continue;
				DirectoryInfo nextTargetDir = target.CreateSubdirectory(diSourceDir.Name);
				CopyDirectory(diSourceDir, nextTargetDir, skip);
			}
		}
		public static void DeleteDirectory(string directory)
		{
			const int retries = 10;
			for (int i = 0; i < retries; i++)
			{
				try
				{
					if (Directory.Exists(directory) == false)
						return;

					try
					{
						File.SetAttributes(directory, FileAttributes.Normal);
					}
					catch (IOException)
					{
					}
					Directory.Delete(directory, true);
					return;
				}
				catch (IOException)
				{
					foreach (var childDir in Directory.GetDirectories(directory))
					{
						try
						{
							File.SetAttributes(childDir, FileAttributes.Normal);
						}
						catch (IOException)
						{
						}
					}
					if (i == retries - 1)// last try also failed
						throw;
					Thread.Sleep(100);
				}
			}
		}


		public static string GetMD5Hash(this Stream stream)
		{
			MD5 md5 = new MD5CryptoServiceProvider();
			var retVal = md5.ComputeHash(stream);

			var sb = new StringBuilder();
			for (var i = 0; i < retVal.Length; i++)
			{
				sb.Append(retVal[i].ToString("x2"));
			}

			return sb.ToString();
		}
	}
}