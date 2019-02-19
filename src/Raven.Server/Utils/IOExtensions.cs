using System;
using System.IO;
using System.Text;
using System.Threading;
using Raven.Client.Extensions;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Utils
{
    public static class IOExtensions
    {
        private const int Retries = 50;

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

        public static void RenameFile(string oldFile, string newFile)
        {
            File.Move(oldFile, newFile);

            if (PlatformDetails.RunningOnPosix)
            {
                Syscall.FsyncDirectoryFor(newFile);
            }
        }

        public static bool EnsureReadWritePermissionForDirectory(string directory)
        {
            string tmpFileName = null;
            string testString = "I can write here!";
            try
            {
                if (string.IsNullOrWhiteSpace(directory))
                {
                    return false;
                }

                if (Directory.Exists(directory) == false)
                {
                    Directory.CreateDirectory(directory);
                }

                tmpFileName = Path.Combine(directory, Path.GetRandomFileName());
                File.WriteAllText(tmpFileName, testString);
                var read = File.ReadAllText(tmpFileName); //I can read too!
                File.Delete(tmpFileName);
                return read == testString;
            }
            catch
            {
                //We need to try and delete the file here too since we can't modify the return value from a finally block.                
                try
                {
                    if (File.Exists(tmpFileName))
                    {
                        File.Delete(tmpFileName);
                    }
                }
                catch
                {
                }                
                return false;
            }
        }

        public static void MoveDirectory(string src, string dst)
        {
            for (var i = 0; i < Retries; i++)
            {
                try
                {
                    DeleteDirectory(dst);

                    SetDirectoryAttributes(src, FileAttributes.Normal);
                    SetDirectoryAttributes(dst, FileAttributes.Normal);

                    Directory.Move(src, dst);
                    return;
                }
                catch (Exception)
                {
                    if (i == Retries - 1)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();

                    Thread.Sleep(100);
                }
            }
        }

        public static void DeleteDirectory(string directory)
        {
            for (var i = 0; i < Retries; i++)
            {
                try
                {
                    if (Directory.Exists(directory) == false)
                        return;

                    SetDirectoryAttributes(directory, FileAttributes.Normal);

                    Directory.Delete(directory, true);
                    return;
                }
                catch (IOException e)
                {
                    try
                    {
                        foreach (var childDir in Directory.GetDirectories(directory, "*", SearchOption.AllDirectories))
                        {
                            SetDirectoryAttributes(childDir, FileAttributes.Normal);
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
            if (i == Retries - 1) // last try also failed
            {
                foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                {
                    var path = Path.GetFullPath(file);
                    try
                    {
                        File.Delete(path);
                    }
                    catch (UnauthorizedAccessException ex)
                    {
                        throw new IOException(WhoIsLocking.ThisFile(path), ex);
                    }
                    catch (IOException ex)
                    {
                        var processesUsingFiles = WhoIsLocking.GetProcessesUsingFile(path);
                        if (processesUsingFiles.Count == 0)
                            throw new IOException("Unable to figure out who is locking " + path, ex);

                        var stringBuilder = new StringBuilder();
                        stringBuilder.Append("The following processes are locking ").Append(path).AppendLine();
                        foreach (var processesUsingFile in processesUsingFiles)
                        {
                            stringBuilder.Append(" ").Append(processesUsingFile.ProcessName).Append(' ').Append(processesUsingFile.Id).
                                AppendLine();
                        }
                        throw new IOException(stringBuilder.ToString(), ex);
                    }
                }
                throw new IOException("Could not delete " + Path.GetFullPath(directory), e);
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();

            Thread.Sleep(100);
        }

        private static void SetDirectoryAttributes(string path, FileAttributes attributes)
        {
            if (Directory.Exists(path) == false)
                return;

            try
            {
                File.SetAttributes(path, attributes);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
