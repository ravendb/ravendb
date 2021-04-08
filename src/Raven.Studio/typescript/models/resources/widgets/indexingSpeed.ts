import generalUtils = require("common/generalUtils");

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");

class indexingSpeed extends historyAwareNodeStats<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload> {

    indexedPerSecond = this.conditionalDataExtractor(x => indexingSpeed.formatNumber(x.IndexedPerSecond));
    mappedPerSecond = this.conditionalDataExtractor(x => indexingSpeed.formatNumber(x.MappedPerSecond));
    reducedPerSecond = this.conditionalDataExtractor(x => indexingSpeed.formatNumber(x.ReducedPerSecond));
    
    private static formatNumber(value: number) {
        if (value < 0.001) {
            return "0";
        }
        if (value < 1) {
            return generalUtils.formatNumberToStringFixed(value, 2);
        }
        return generalUtils.formatNumberToStringFixed(value, 0);
    }
}


export = indexingSpeed;
