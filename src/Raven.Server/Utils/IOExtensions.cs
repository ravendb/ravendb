using System;
using System.IO;
using System.Text;
using System.Threading;
using Raven.Server.Extensions;

namespace Raven.Server.Utils
{
    public static class IOExtensions
    {
        const int retries = 10;

        public static string ToFullPath(this string path, string basePath = null)
        {
            if (String.IsNullOrWhiteSpace(path))
                return String.Empty;
            path = Environment.ExpandEnvironmentVariables(path);
            if (path.StartsWith(@"~\") || path.StartsWith(@"~/"))
            {
                if (!string.IsNullOrEmpty(basePath))
                    basePath = Path.GetDirectoryName(basePath.EndsWith("\\") ? basePath.Substring(0, basePath.Length - 2) : basePath);

                path = Path.Combine(basePath ?? AppContext.BaseDirectory, path.Substring(2));
            }

            return Path.IsPathRooted(path) ? path : Path.Combine(basePath ?? AppContext.BaseDirectory, path);
        }

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

            GC.Collect();
            GC.WaitForPendingFinalizers();

            Thread.Sleep(100);
        }

    }
}