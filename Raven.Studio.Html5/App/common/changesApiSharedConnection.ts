interface sharedChangesConnection {
    databases: sharedChangesConnectionDatabase[];
} 

interface sharedChangesConnectionDatabase {
    id: string;
    name: string;
    lastHeartbeatMs: number;
    events: sharedChangesConnectionEvent[];
}

interface sharedChangesConnectionEvent {
    eventJson: string;
    time: number;
}