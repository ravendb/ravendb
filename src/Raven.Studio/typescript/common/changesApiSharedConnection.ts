interface changesApiEventQueue {
    ownerId: string;
    name: string;
    lastHeartbeatMs: number;
    events: changesApiEvent[];
}

interface changesApiEvent {
    dtoJson: string;
    time: number;
}
