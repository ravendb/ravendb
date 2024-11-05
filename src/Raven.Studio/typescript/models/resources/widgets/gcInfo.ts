import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import genUtils from "common/generalUtils";
import GcMemoryInfo = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo;

class gcInfo extends historyAwareNodeStats<GcInfoNormalizedData> {
    pauseFormatted = this.conditionalDataExtractor(x => x.memoryInfo.PauseTimePercentage.toFixed(2) + "%");
    type = this.conditionalDataExtractor(x => x.gcType);
    concurrent = this.conditionalDataExtractor(x => x.memoryInfo.Concurrent);
    compacted = this.conditionalDataExtractor(x => x.memoryInfo.Compacted);
    generation = this.conditionalDataExtractor(x => x.memoryInfo.Generation.toString()); 
    memoryFormatted = this.conditionalDataExtractor(x => genUtils.formatBytesToSize(x.memoryInfo.TotalHeapSizeAfterBytes));
    
    genProvider = (sizeAccessor: (memoryInfo: GcMemoryInfo) => Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize) => {
        return this.conditionalDataExtractor(x => {
            const size = sizeAccessor(x.memoryInfo);
            const before = genUtils.formatBytesToSize(size.SizeBeforeBytes);
            const after = genUtils.formatBytesToSize(size.SizeAfterBytes);
            return before + "→" + after;
        })
    }
    
    gen0Formatted = this.genProvider(x => x.Gen0HeapSize);
    gen1Formatted = this.genProvider(x => x.Gen1HeapSize);
    gen2Formatted = this.genProvider(x => x.Gen2HeapSize);
    pinnedFormatted = this.genProvider(x =>  x.PinnedObjectHeapSize);
    lohFormatted = this.genProvider(x => x.LargeObjectHeapSize);
    
    pause1 = this.conditionalDataExtractor(x => {
        const num = x.memoryInfo.PauseDurationsInMs[0];
        return Math.round(num) + " ms";
    });
    
    pause2 = this.conditionalDataExtractor(x => {
        const num = x.memoryInfo.PauseDurationsInMs[1];
        if (num) {
            return Math.round(num) + " ms";    
        } 
        return "n/a";
    });
    
    constructor(tag: string) {
        super(tag, "exact");
    }

    protected noDataText(): string|null {
        const currentItem = this.currentItem();
        return currentItem ? null : "No data";
    }
}


export = gcInfo;
