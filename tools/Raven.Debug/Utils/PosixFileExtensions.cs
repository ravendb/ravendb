using System;
using System.Diagnostics;
using Sparrow.Platform;

namespace Raven.Debug.Utils;

public static class PosixFileExtensions
{
    public static void ChangeFileOwner(string fileName, string owner)
    {
        if (PlatformDetails.RunningOnPosix == false)
            throw new InvalidOperationException("This method is supposed to be called only on Posix systems");

        var startup = new ProcessStartInfo
        {
            FileName = "chown",
            Arguments = $"{owner}:{owner} {fileName}",
            UseShellExecute = false,
            RedirectStandardOutput = false,
            RedirectStandardError = true
        };

        var process = new Process
        {
            StartInfo = startup,
            EnableRaisingEvents = true
        };

        process.Start();
        process.WaitForExit();


        if (process.ExitCode == 0) 
            return;
        
        var error = process.StandardError.ReadToEnd();

        throw new InvalidOperationException($"Error changing file owner: {error}");
    }
}
