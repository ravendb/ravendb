import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import genUtils from "common/generalUtils";
import GcMemoryInfo = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo;

class gcInfo extends historyAwareNodeStats<GcInfoNormalizedData> {
    pauseFormatted = this.conditionalDataExtractor(x => x.memoryInfo.PauseTimePercentage.toFixed(2) + "%");
    pauseLevel = this.conditionalDataExtractor(x => {
        const percentage = x.memoryInfo.PauseTimePercentage;
        if (percentage >= 10) {
            return "danger";
        } else if (percentage >= 5) {
            return "warning";
        }
        
        return null;
    })
    type = this.conditionalDataExtractor(x => x.gcType);
    concurrent = this.conditionalDataExtractor(x => x.memoryInfo.Concurrent);
    compacted = this.conditionalDataExtractor(x => x.memoryInfo.Compacted);
    generation = this.conditionalDataExtractor(x => x.memoryInfo.Generation.toString()); 
    memoryFormatted = this.conditionalDataExtractor(x => genUtils.formatBytesToSize(x.memoryInfo.TotalHeapSizeAfterBytes));
    
    fragmentationInfo = ko.pureComputed(() => {
        const noData = this.noDataText();
        if (noData) {
            return null;
        }
        
        const fragmentationGen0 = this.gen0FragmentationFormatted();
        const fragmentationGen1 = this.gen1FragmentationFormatted();
        const fragmentationGen2 = this.gen2FragmentationFormatted();
        const fragmentationLoh = this.lohFragmentationFormatted();
        const fragmentationPoh = this.pinnedFragmentationFormatted();
        
        return `<div>
                    <h3 class="text-center">Heap Fragmentation</h3>
                    <div class="details-item gen-0">
                        <div class="details-item-name">Gen0 <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationGen0}</div>
                    </div>
                    <div class="details-item gen-1">
                        <div class="details-item-name">Gen1 <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationGen1}</div>
                    </div>
                    <div class="details-item gen-2">
                        <div class="details-item-name">Gen2 <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationGen2}</div>
                    </div>
                    <div class="details-item loh">
                        <div class="details-item-name" title="Large object heap">LOH <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationLoh}</div>
                    </div>
                    <div class="details-item pinned">
                        <div class="details-item-name" title="Pinned object heap">POH <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationPoh}</div>
                    </div>
                </div>`
    })
    
    genProvider = (sizeAccessor: (memoryInfo: GcMemoryInfo) => Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize) => {
        return this.conditionalDataExtractor(x => {
            const size = sizeAccessor(x.memoryInfo);
            const before = genUtils.formatBytesToSize(size.SizeBeforeBytes);
            const after = genUtils.formatBytesToSize(size.SizeAfterBytes);
            return before + "→" + after;
        })
    }
    
    fragmentationProvider = (sizeAccessor: (memoryInfo: GcMemoryInfo) => Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize) => {
        return this.conditionalDataExtractor(x => {
            const size = sizeAccessor(x.memoryInfo);
            const before = genUtils.formatBytesToSize(size.FragmentationBeforeBytes);
            const after = genUtils.formatBytesToSize(size.FragmentationAfterBytes);
            return before + "→" + after;
        })
    }
    
    gen0Formatted = this.genProvider(x => x.Gen0HeapSize);
    gen1Formatted = this.genProvider(x => x.Gen1HeapSize);
    gen2Formatted = this.genProvider(x => x.Gen2HeapSize);
    lohFormatted = this.genProvider(x => x.LargeObjectHeapSize);
    pinnedFormatted = this.genProvider(x =>  x.PinnedObjectHeapSize);

    gen0FragmentationFormatted = this.fragmentationProvider(x => x.Gen0HeapSize);
    gen1FragmentationFormatted = this.fragmentationProvider(x => x.Gen1HeapSize);
    gen2FragmentationFormatted = this.fragmentationProvider(x => x.Gen2HeapSize);
    lohFragmentationFormatted = this.fragmentationProvider(x => x.LargeObjectHeapSize);
    pinnedFragmentationFormatted = this.fragmentationProvider(x =>  x.PinnedObjectHeapSize);
    
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
