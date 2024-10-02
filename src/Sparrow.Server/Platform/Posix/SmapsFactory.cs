using System.Diagnostics;
using System;
using System.IO;
using Sparrow.Platform;

namespace Sparrow.Server.Platform.Posix;

internal static class SmapsFactory
{
    public const int BufferSize = 4096;

    public static SmapsReaderType DefaultSmapsReaderType = SmapsReaderType.Smaps;

    static SmapsFactory()
    {
        var envSmapsReaderType = Environment.GetEnvironmentVariable("RAVEN_SMAPS_READER_TYPE");

        if (PlatformDetails.RunningOnPosix == false)
            return;

        if (string.IsNullOrWhiteSpace(envSmapsReaderType) == false && Enum.TryParse<SmapsReaderType>(envSmapsReaderType, ignoreCase: true, out var smapsReaderType))
        {
            DefaultSmapsReaderType = smapsReaderType;
            return;
        }

        using (var process = Process.GetCurrentProcess())
        {
            if (File.Exists(SmapsRollupReader.GetSmapsPath(process.Id)))
                DefaultSmapsReaderType = SmapsReaderType.SmapsRollup;
        }
    }

    public static ISmapsReader CreateSmapsReader(byte[][] smapsBuffer)
    {
        return CreateSmapsReader(DefaultSmapsReaderType, smapsBuffer);
    }

    public static ISmapsReader CreateSmapsReader(SmapsReaderType type, byte[][] smapsBuffer)
    {
        switch (type)
        {
            case SmapsReaderType.Smaps:
                return new SmapsReader(smapsBuffer);
            case SmapsReaderType.SmapsRollup:
                return new SmapsRollupReader(smapsBuffer);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}
