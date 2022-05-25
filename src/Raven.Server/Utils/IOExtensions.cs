using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Client.Exceptions;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Utils
{
    public static class IOExtensions
    {
        private const int Retries = 50;

        public static EventHandler<(string Path, TimeSpan Duration, int Attempt)> AfterGc;

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

                    RunGc(src, i);
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

        public static void CreateDirectory(string directory)
        {
            for (var i = 0; i < Retries; i++)
            {
                try
                {
                    if (Directory.Exists(directory))
                        return;

                    Directory.CreateDirectory(directory);
                    return;
                }
                catch (Exception)
                {
                    if (i == Retries - 1)
                        throw;

                    RunGc(directory, i);
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
                    catch (Exception ex)
                    {
                        var message = UnsuccessfulFileAccessException.GetMessage(path, FileAccess.Write, ex);
                        throw new IOException(message, ex);
                    }
                }
                throw new IOException("Could not delete " + Path.GetFullPath(directory), e);
            }

            RunGc(directory, i);
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

        private static void RunGc(string path, int attempt)
        {
            Stopwatch sw = null;

            if (AfterGc != null)
                sw = Stopwatch.StartNew();

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (AfterGc != null)
                AfterGc(null, (path, sw.Elapsed, attempt));

            Thread.Sleep(100);
        }
    }
}
