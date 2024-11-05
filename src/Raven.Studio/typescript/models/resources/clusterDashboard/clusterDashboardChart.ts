export interface clusterDashboardChart<TPayload extends { Date: string }> {
    highlightTime: (date: ClusterWidgetAlignedDate|null) => void;
    recordNoData: (time: Date, key: string) => void;
    draw: () => void;
    onResize: () => void;
    onData: (key: string, payload: TPayload) => void;
    onHeartbeat: (date: Date) => void;
}
