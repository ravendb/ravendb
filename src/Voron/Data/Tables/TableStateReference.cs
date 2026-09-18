using Voron.Data.RawData;

namespace Voron.Data.Tables;

// state that must be single per physical table within a transaction; a table can be opened through two Table instances (one per schema.Compressed)
public class TableStateReference
{
    public long NumberOfEntries { get; set; }

    public long OverflowPageCount { get; set; }

    public ActiveRawDataSmallSection ActiveDataSmallSection { get; set; }
}
