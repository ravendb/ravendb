using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Recovery
{
    public class VoronRecoveryConfiguration
    {
        public string PathToDataFile { get; set; }
        public string DataFileDirectory { get; set; }
        public string OutputFileName { get; set; }
        public int PageSizeInKb { get; set; } = 4;
        public int NumberOfFiledsInDocumentTable { get; set; } = 5;
        public int InitialContextSizeInMB { get; set; } = 1;
        public int InitialContextLongLivedSizeInKB { get; set; } = 16;
        public int ProgressIntervalInSeconds { get; set; } = 5;
        public bool DisableCopyOnWriteMode { get; set; }

        public static VoronRecoveryArgsProcessStatus ProcessArgs(string[] args, out VoronRecoveryConfiguration config)
        {
            config = null;
            if (args.Length < 2)
                return VoronRecoveryArgsProcessStatus.NotEnoughArguments;
            if (!Directory.Exists(args[0]) || !File.Exists(Path.Combine(args[0], DatafileName)))
                return VoronRecoveryArgsProcessStatus.MissingDataFile;
            try
            {
                if (!Directory.Exists(args[1]))
                    Directory.CreateDirectory(args[1]);
                var testFile = Path.Combine(args[1], RecoveryFileName);
                File.WriteAllText(testFile, "I have write permission!");
                File.Delete(testFile);
            }
            catch
            {
                return VoronRecoveryArgsProcessStatus.CantWriteToOutputDirectory;
            }
            config = new VoronRecoveryConfiguration()
            {
                DataFileDirectory = args[0],
                PathToDataFile = Path.Combine(args[0], DatafileName),
                OutputFileName = Path.Combine(args[1], RecoveryFileName)
            };
            if (args.Length > 2 && args.Length % 2 != 0)
            {
                return VoronRecoveryArgsProcessStatus.WrongNumberOfArgs;
            }
            for (var i = 2; i < args.Length; i += 2)
            {
                switch (args[i])
                {
                    case "-OutputFileName":
                        config.OutputFileName = Path.Combine(args[1], args[i + 1]);
                        break;
                    case "-PageSizeInKB":
                        int pageSize;
                        if (int.TryParse(args[i + 1], out pageSize) == false || pageSize < 1)
                            return VoronRecoveryArgsProcessStatus.InvalidPageSize;
                        config.PageSizeInKb = pageSize;
                        break;
                    case "-TableValueEntries":
                        int tvrCount;
                        if (int.TryParse(args[i + 1], out tvrCount) == false || tvrCount < 1)
                            return VoronRecoveryArgsProcessStatus.InvalidTableValueCount;
                        config.NumberOfFiledsInDocumentTable = tvrCount;
                        break;
                    case "-InitialContextSizeInMB":
                        int contextSize;
                        if (int.TryParse(args[i + 1], out contextSize) == false || contextSize < 1)
                            return VoronRecoveryArgsProcessStatus.InvalidContextSize;
                        config.InitialContextSizeInMB = contextSize;
                        break;
                    case "-InitialContextLongLivedSizeInKB":
                        int longLivedContextSize;
                        if (int.TryParse(args[i + 1], out longLivedContextSize) == false || longLivedContextSize < 1)
                            return VoronRecoveryArgsProcessStatus.InvalidLongLivedContextSize;
                        config.InitialContextLongLivedSizeInKB = longLivedContextSize;
                        break;
                    case "-RefreshRateInSeconds":
                        int refreshRate;
                        if (int.TryParse(args[i + 1], out refreshRate) == false || refreshRate < 1)
                            return VoronRecoveryArgsProcessStatus.InvalidRefreshRate;
                        config.ProgressIntervalInSeconds = refreshRate;
                        break;
                    case "-DisableCopyOnWriteMode":
                        bool disableCopyOnWriteMode;
                        if (bool.TryParse(args[i + 1], out disableCopyOnWriteMode) == false)
                            return VoronRecoveryArgsProcessStatus.BadArg;
                        config.DisableCopyOnWriteMode = disableCopyOnWriteMode;
                        break;
                    default:
                        return VoronRecoveryArgsProcessStatus.BadArg;
                }
            }
            return VoronRecoveryArgsProcessStatus.Success;
        }

        private const string DatafileName = "Raven.voron";
        private const string RecoveryFileName = "recovery.ravendump";
        public enum VoronRecoveryArgsProcessStatus
        {
            Success,
            NotEnoughArguments,
            MissingDataFile,
            CantWriteToOutputDirectory,
            WrongNumberOfArgs,
            InvalidPageSize,
            InvalidTableValueCount,
            InvalidContextSize,
            InvalidLongLivedContextSize,
            InvalidRefreshRate,
            BadArg
        }
    }
}
