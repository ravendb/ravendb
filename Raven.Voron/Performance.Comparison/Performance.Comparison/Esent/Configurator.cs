namespace Performance.Comparison.Esent
{
    using System;
    using System.IO;

    using Microsoft.Isam.Esent.Interop;

    public class Configurator
    {
        public const int MaxSessions = 256;

        public InstanceParameters ConfigureInstance(JET_INSTANCE jetInstance, string path)
        {
            path = Path.GetFullPath(path);
            var logsPath = path;
            var circularLog = true;
            var logFileSizeInMb = 64;
            logFileSizeInMb = Math.Max(1, logFileSizeInMb / 4);
            var maxVerPages = 512;
            var maxVerPagesResult = TranslateToSizeInVersionPages(maxVerPages, 1024 * 1024);
            var instanceParameters = new InstanceParameters(jetInstance)
            {
                CircularLog = circularLog,
                Recovery = true,
                NoInformationEvent = false,
                CreatePathIfNotExist = true,
                EnableIndexChecking = true,
                TempDirectory = Path.Combine(logsPath, "temp"),
                SystemDirectory = Path.Combine(logsPath, "system"),
                LogFileDirectory = Path.Combine(logsPath, "logs"),
                MaxVerPages = maxVerPagesResult,
                PreferredVerPages = TranslateToSizeInVersionPages((int)(maxVerPagesResult * 0.85), 1024 * 1024),
                BaseName = "RVN",
                EventSource = "EsentTest",
                LogBuffers = TranslateToSizeInDatabasePages(8192, 1024),
                LogFileSize = (logFileSizeInMb * 1024),
                MaxSessions = MaxSessions,
                MaxCursors = 2048,
                DbExtensionSize = TranslateToSizeInDatabasePages(8, 1024 * 1024),
                AlternateDatabaseRecoveryDirectory = path
            };

            if (Environment.OSVersion.Version >= new Version(5, 2))
            {
                // JET_paramEnableIndexCleanup is not supported on WindowsXP
                const int JET_paramEnableIndexCleanup = 54;
                Api.JetSetSystemParameter(jetInstance, JET_SESID.Nil, (JET_param)JET_paramEnableIndexCleanup, 1, null);
            }

            return instanceParameters;
        }

        public void LimitSystemCache()
        {
            var defaultCacheSize = Environment.Is64BitProcess ? Math.Min(1024, (1024 / 4)) : 256;
            int cacheSizeMaxInMegabytes = defaultCacheSize;
            int cacheSizeMax = TranslateToSizeInDatabasePages(cacheSizeMaxInMegabytes, 1024 * 1024);
            if (SystemParameters.CacheSizeMax > cacheSizeMax)
            {
                SystemParameters.CacheSizeMax = cacheSizeMax;
            }
        }

        private static int TranslateToSizeInDatabasePages(int sizeInMegabytes, int multiply)
        {
            //This doesn't suffer from overflow, do the division first (to make the number smaller) then multiply afterwards
            double tempAmt = (double)sizeInMegabytes / SystemParameters.DatabasePageSize;
            int finalSize = (int)(tempAmt * multiply);
            return finalSize;
        }

        private static int TranslateToSizeInVersionPages(int sizeInMegabytes, int multiply)
        {
            //This doesn't suffer from overflow, do the division first (to make the number smaller) then multiply afterwards
            double tempAmt = (double)sizeInMegabytes / GetVersionPageSize();
            int finalSize = (int)(tempAmt * multiply);
            return finalSize;
        }

        private static Lazy<int> VersionPageSize = new Lazy<int>(() =>
        {
            // see discussion here: http://managedesent.codeplex.com/discussions/405939
            const int JET_paramVerPageSize = 128;
            int versionPageSize = 0;
            string paramString;
            try
            {
                Api.JetGetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, (JET_param)JET_paramVerPageSize, ref versionPageSize,
                                          out paramString, 0);
            }
            catch (EsentErrorException e)
            {
                if (e.Error == JET_err.InvalidParameter) // Win 2003 error
                    versionPageSize = 16 * 1024; // default size
                else
                    throw;
            }

            versionPageSize = Math.Max(versionPageSize, SystemParameters.DatabasePageSize * 2);

            if (Environment.Is64BitProcess)
            {
                versionPageSize *= 2;
            }
            return Math.Min(versionPageSize, 64 * 1024);
        });

        public static int GetVersionPageSize()
        {
            return VersionPageSize.Value;
        }
    }
}