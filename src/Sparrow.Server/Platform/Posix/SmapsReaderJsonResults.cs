using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Sparrow.Server.Platform.Posix;

internal struct SmapsReaderJsonResults : ISmapsReaderResultAction
{
    private DynamicJsonArray _dja;

    public void Add(SmapsReaderResults results)
    {
        var djv = new DynamicJsonValue
        {
            ["File"] = results.ResultString,
            ["Size"] = Sizes.Humane(results.Size),
            ["Rss"] = Sizes.Humane(results.Rss),
            ["SharedClean"] = Sizes.Humane(results.SharedClean),
            ["SharedDirty"] = Sizes.Humane(results.SharedDirty),
            ["PrivateClean"] = Sizes.Humane(results.PrivateClean),
            ["PrivateDirty"] = Sizes.Humane(results.PrivateDirty),
            ["TotalClean"] = results.SharedClean + results.PrivateClean,
            ["TotalCleanHumanly"] = Sizes.Humane(results.SharedClean + results.PrivateClean),
            ["TotalDirty"] = results.SharedDirty + results.PrivateDirty,
            ["TotalDirtyHumanly"] = Sizes.Humane(results.SharedDirty + results.PrivateDirty),
            ["TotalSwap"] = results.Swap,
            ["TotalSwapHumanly"] = Sizes.Humane(results.Swap)
        };
        if (_dja == null)
            _dja = new DynamicJsonArray();
        _dja.Add(djv);
    }

    public DynamicJsonArray ReturnResults()
    {
        return _dja;
    }
}
