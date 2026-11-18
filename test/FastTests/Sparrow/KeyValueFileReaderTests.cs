using System.IO;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow;

public class KeyValueFileReaderTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string MemInfo = "MemTotal:       16000 kB\nMemFree:         2000 kB\nMemAvailable:    9000 kB\nSwapTotal:          0 kB\nCommitted_AS:   12345 kB\n";

    [RavenFact(RavenTestCategory.Core)]
    public void KeysAreFoundInAnyOrder()
    {
        if (PlatformDetails.RunningOnPosix == false)
            return;

        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, MemInfo);
            using var reader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(file);
            reader.ReadFileIntoBuffer();

            // the order of the file, then out of order (the search wraps around)
            Assert.Equal(16000, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("MemTotal:"u8));
            Assert.Equal(9000, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("MemAvailable:"u8));
            Assert.Equal(12345, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("Committed_AS:"u8));
            Assert.Equal(2000, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("MemFree:"u8));
            Assert.Equal(0, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("SwapTotal:"u8));
            Assert.Equal(0, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("Missing:"u8));
            Assert.Equal(16000, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("MemTotal:"u8));

            // a new read starts from the beginning
            File.WriteAllText(file, MemInfo.Replace("2000", "3000"));
            reader.ReadFileIntoBuffer();
            Assert.Equal(3000, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("MemFree:"u8));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenFact(RavenTestCategory.Core)]
    public void AFileLargerThanTheBufferIsReadWhole()
    {
        if (PlatformDetails.RunningOnPosix == false)
            return;

        var file = Path.GetTempFileName();
        try
        {
            var lines = new System.Text.StringBuilder();
            for (var i = 0; i < 2_000; i++)
                lines.Append("padding_").Append(i).Append(' ').Append(i).Append('\n'); // about 30 KB
            lines.Append("Last: 42\n");
            File.WriteAllText(file, lines.ToString());

            using var reader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(file);
            reader.ReadFileIntoBuffer();
            Assert.Equal(42, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("Last:"u8));
            Assert.Equal(1999, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("padding_1999 "u8));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenFact(RavenTestCategory.Core)]
    public void AKeyMatchesOnlyAtTheStartOfALine()
    {
        if (PlatformDetails.RunningOnPosix == false)
            return;

        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "nr_inactive_file 300\nnr_active_file 700\n");
            using var reader = new KernelVirtualFileSystemUtils.BufferedPosixKeyValueOutputValueReader(file);
            reader.ReadFileIntoBuffer();
            Assert.Equal(700, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("nr_active_file "u8));
            Assert.Equal(0, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("active_file "u8));
            Assert.Equal(300, reader.ExtractNumericValueFromKeyValuePairsFormattedFile("nr_inactive_file "u8));
        }
        finally
        {
            File.Delete(file);
        }
    }
}
