using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Sparrow.Server.Compression;
using Voron.Data;
using Voron.Data.CompactTrees;
using Voron.Global;

int pagesToUse = 8;
int tableSize = pagesToUse * Constants.Storage.PageSize - PageHeader.SizeOf - PersistentDictionaryHeader.SizeOf;

var data = new byte[tableSize];
unsafe
{
    // Find all .txt files in the current directory
    var files = Directory.GetFiles(Directory.GetCurrentDirectory(), "*.txt");

    var dictionary = new List<string>();
    foreach (var file in files)
    {
        foreach (var line in File.ReadAllLines(file))
            dictionary.Add(line);
    }

    fixed (byte* dataPtr = data)
    {
        var encoder = new HopeEncoder<Encoder3Gram<NativeMemoryEncoderState>>(
        new Encoder3Gram<NativeMemoryEncoderState>(
            new NativeMemoryEncoderState(dataPtr, tableSize)));

        int lowerBound = 128;
        int higherBound = short.MaxValue - 1;

        while (lowerBound < higherBound)
        {
            int current = (lowerBound + higherBound) / 2;
            if (current == lowerBound)
                break;

            try
            {
                encoder.Train(new StringArrayIterator(dictionary.ToArray()), current);
                lowerBound = current;
            }
            catch
            {
                higherBound = current - 1;
                continue;
            }
        }

        var treesDirectory = new DirectoryInfo("..\\..\\..\\..\\..\\src\\Voron\\Data\\CompactTrees");

        var dictionarySize = lowerBound;
        encoder.Train(new StringArrayIterator(dictionary.ToArray()), dictionarySize);
        var output = File.OpenWrite(Path.Combine(treesDirectory.FullName, "dictionary.bin"));

        Span<int> tableSizeSpan = stackalloc int[1];
        tableSizeSpan[0] = tableSize;
        output.Write(MemoryMarshal.Cast<int, byte>(tableSizeSpan));
        output.Write(new ReadOnlySpan<byte>(dataPtr, tableSize));

        string fileContent = $@"
/// DO NOT MODIFY.
/// All changes should be done through the usage of the Voron.Dictionary.Generator

namespace Voron.Data.CompactTrees;
partial class PersistentDictionary
{{
    private const int NumberOfPagesForDictionary = { pagesToUse };
    public const int MaxDictionaryEntries = { dictionarySize };
    private const int DefaultDictionaryTableSize = { tableSize };
}}";

        File.WriteAllText(Path.Combine(treesDirectory.FullName, "PersistentDictionary.Generated.cs"), fileContent);

        Console.WriteLine($"Optimal dictionary size for the table size: {dictionarySize}.\n");
        Console.WriteLine($"Press enter to continue...");
        Console.ReadLine();
    }
}

public struct StringArrayIterator : IReadOnlySpanIndexer, IReadOnlySpanEnumerator
{
    private int _currentIdx = 0;
    private readonly string[] _values;

    public StringArrayIterator(string[] values)
    {
        _values = values;
    }

    public int Length => _values.Length;

    public ReadOnlySpan<byte> this[int i] => Encoding.UTF8.GetBytes(_values[i]);

    public void Reset()
    {
        _currentIdx = 0;
    }

    public bool MoveNext(out ReadOnlySpan<byte> result)
    {
        if (_currentIdx >= _values.Length)
        {
            result = default;
            return false;
        }
        
        result = Encoding.UTF8.GetBytes(_values[_currentIdx++]);
        return true;
    }
}


