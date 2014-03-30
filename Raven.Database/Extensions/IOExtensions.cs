//-----------------------------------------------------------------------
// <copyright file="IOExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

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
						var processesUsingFiles = WhoIsLocking.GetProcessesUsingFile(path);
						var stringBuilder = new StringBuilder();
						stringBuilder.Append("The following processing are locking ").Append(path).AppendLine();
						foreach (var processesUsingFile in processesUsingFiles)
						{
							stringBuilder.Append("\t").Append(processesUsingFile.ProcessName).Append(' ').Append(processesUsingFile.Id).
								AppendLine();
						}
						throw new IOException(stringBuilder.ToString());
					}
					catch(IOException)
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
				path = Path.Combine(basePath  ?? AppDomain.CurrentDomain.BaseDirectory, path.Substring(2));

			return Path.IsPathRooted(path) ? path : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, path);
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
				DirectoryInfo nextTargetDir = target.CreateSubdirectory(diSourceDir.Name);
				CopyDirectory(diSourceDir, nextTargetDir);
			}
		}
	}
}
