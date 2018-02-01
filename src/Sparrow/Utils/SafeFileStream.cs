using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Sparrow.Utils
{
    public static class SafeFileStream
    {
        private static void AvoidKillingTheProcessWhenOutOfMemory(FileStream file)
        {
            GC.SuppressFinalize(file); // See RavenDB-10376 and https://github.com/dotnet/corefx/issues/26734
        }
        
        public static FileStream Create(string path, FileMode mode)
        {
            var file = new FileStream(path, mode);
            AvoidKillingTheProcessWhenOutOfMemory(file);
            return file;
        }
        
        public static FileStream Create(string path, FileMode mode, FileAccess access)
        {
            var file = new FileStream(path, mode, access);
            AvoidKillingTheProcessWhenOutOfMemory(file);
            return file;
        }

        public static FileStream Create(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions options)
        {
            var file = new FileStream(path, mode, access, share, bufferSize, options);
            AvoidKillingTheProcessWhenOutOfMemory(file);
            return file;
        }

        public static FileStream Create(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, bool useAsync)
        {
            var file = new FileStream(path, mode, access, share, bufferSize, useAsync);
            AvoidKillingTheProcessWhenOutOfMemory(file);
            return file;
        }

        public static FileStream Create(string path, FileMode mode, FileAccess access, FileShare share)
        {
            var file = new FileStream(path, mode, access, share);
            AvoidKillingTheProcessWhenOutOfMemory(file);
            return file;
        }

        public static FileStream Create(SafeFileHandle handle, FileAccess access)
        {
            var file = new FileStream(handle, access);
            AvoidKillingTheProcessWhenOutOfMemory(file);
            return file;
        }
    }
}
