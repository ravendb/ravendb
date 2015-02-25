//-----------------------------------------------------------------------
// <copyright file="IOExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Abstractions.Util.Encryptors;

namespace Raven.Database.Extensions
{
	using Raven.Abstractions.Util;

	public static class IOExtensions
	{
		const int retries = 10;

		public static void DeleteFile(string file)
		{
			try
			{
				File.Delete(file);
			}
			catch (IOException)
			{

			}
			catch (UnauthorizedAccessException)
			{

			}
		}

		public static void DeleteDirectory(string directory)
		{
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
					catch (UnauthorizedAccessException)
					{
					}
					Directory.Delete(directory, true);
					return;
				}
				catch (IOException e)
				{
					try
					{
						foreach (var childDir in Directory.GetDirectories(directory, "*", SearchOption.AllDirectories))
						{
							try
							{
								File.SetAttributes(childDir, FileAttributes.Normal);
							}
							catch (IOException)
							{
							}
							catch (UnauthorizedAccessException)
							{
							}
						}
					}
					catch (IOException)
					{
					}
					catch (UnauthorizedAccessException)
					{
					}

					TryHandlingError(directory, i, e);
				}
				catch (UnauthorizedAccessException e)
				{
					TryHandlingError(directory, i, e);
				}
			}
		}

		private static void TryHandlingError(string directory, int i, Exception e)
		{
			if (i == retries - 1) // last try also failed
			{
				foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
				{
					var path = Path.GetFullPath(file);
					try
					{
						File.Delete(path);
					}
					catch (UnauthorizedAccessException)
					{
						throw new IOException(WhoIsLocking.ThisFile(path));
					}
					catch (IOException)
					{
						var processesUsingFiles = WhoIsLocking.GetProcessesUsingFile(path);
						var stringBuilder = new StringBuilder();
						stringBuilder.Append("The following processes are locking ").Append(path).AppendLine();
						foreach (var processesUsingFile in processesUsingFiles)
						{
							stringBuilder.Append(" ").Append(processesUsingFile.ProcessName).Append(' ').Append(processesUsingFile.Id).
								AppendLine();
						}
						throw new IOException(stringBuilder.ToString());
					}
				}
				throw new IOException("Could not delete " + Path.GetFullPath(directory), e);
			}

			RavenGC.CollectGarbage(true);
			Thread.Sleep(100);
		}

		public static string ToFullPath(this string path, string basePath = null)
		{
			if (String.IsNullOrWhiteSpace(path))
				return String.Empty;
			path = Environment.ExpandEnvironmentVariables(path);
			if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
			{
				if (!string.IsNullOrEmpty(basePath))
					basePath = Path.GetDirectoryName(basePath.EndsWith("\\") ? basePath.Substring(0, basePath.Length - 2) : basePath);

				path = Path.Combine(basePath ?? AppDomain.CurrentDomain.BaseDirectory, path.Substring(2));
			}

			return Path.IsPathRooted(path) ? path : Path.Combine(basePath ?? AppDomain.CurrentDomain.BaseDirectory, path);
		}

		public static string ToFullTempPath(this string path)
		{
			if (string.IsNullOrWhiteSpace(path))
				return string.Empty;
			path = Environment.ExpandEnvironmentVariables(path);
			if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
				path = Path.Combine(Path.GetTempPath(), path.Substring(2));

			return Path.IsPathRooted(path) ? path : Path.Combine(Path.GetTempPath(), path);
		}

		public static void CopyDirectory(string from, string to)
		{
			try
			{
				CopyDirectory(new DirectoryInfo(from), new DirectoryInfo(to));
			}
			catch (Exception e)
			{
				throw new Exception(String.Format("Exception encountered copying directory from {0} to {1}.", from, to), e);
			}
		}

		static void CopyDirectory(DirectoryInfo source, DirectoryInfo target)
		{
			CopyDirectory(source, target, new string[0]);
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
			foreach (DirectoryInfo diSourceDir in source.GetDirectories())
			{
				if (skip.Contains(diSourceDir.Name))
					continue;

				DirectoryInfo nextTargetDir = target.CreateSubdirectory(diSourceDir.Name);
				CopyDirectory(diSourceDir, nextTargetDir, skip);
			}
		}

		public static string GetMD5Hex(byte[] input)
		{
			var sb = new StringBuilder();
			foreach (byte t in input)
				sb.Append(t.ToString("x2"));

			return sb.ToString();
		}

		public static string GetMD5Hash(this Stream stream)
		{
			using (var md5 = Encryptor.Current.CreateHash())
			{
				return GetMD5Hex(md5.Compute16(stream));
			}
		}
	}
}
