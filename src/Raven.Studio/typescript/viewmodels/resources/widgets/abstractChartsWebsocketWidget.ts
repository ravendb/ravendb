import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import abstractTransformingChartsWebsocketWidget
    from "viewmodels/resources/widgets/abstractTransformingChartsWebsocketWidget";

abstract class abstractChartsWebsocketWidget<
    TPayload extends Raven.Server.Dashboard.Cluster.AbstractClusterDashboardNotification, 
    TNodeStats extends historyAwareNodeStats<TPayload>,
    TConfig = unknown, 
    TState = unknown
    > extends abstractTransformingChartsWebsocketWidget<TPayload, TPayload, TPayload & { Key: string }, TNodeStats, TConfig, TState> {
    
}

export = abstractChartsWebsocketWidget;
