import moment from "moment";

export interface ClusterDebugNodeInfo {
    term: number;
    clusterVersion: number;
    localVersion: number;
    role: string;
    lastAppendedTime: string;
    lastCommitedTime: string;
    firstEntryIndex: number;
    lastLogEntryIndex: number;
    commitIndex: number;
    queueSize: number;
    criticalError: Raven.Server.Rachis.RachisConsensus.UnrecoverableClusterError;
    installingSnapshot: boolean;
    chocked: boolean;
    progress: number;
    connections: Raven.Server.Rachis.RaftDebugView.PeerConnection[];
    installationLog: Raven.Server.Rachis.RachisDebugMessage[];
}

// referenceTimeUtc is the "now" the choked heuristic measures the last commit against. Live views leave it
// at the wall-clock default; offline package snapshots pass the snapshot's own reference time so the
// "no commits for over 2 minutes" check isn't tripped just because the package was captured in the past.
export function mapRaftDebugView(
    view: Raven.Server.Rachis.RaftDebugView,
    referenceTimeUtc: moment.Moment = moment.utc()
): ClusterDebugNodeInfo {
    return {
        term: view.Term,
        clusterVersion: view.CommandsVersion.Cluster,
        localVersion: view.CommandsVersion.Local,
        role: view.Role,
        lastAppendedTime: view.Log.LastAppendedTime,
        lastCommitedTime: view.Log.LastCommitedTime,
        firstEntryIndex: view.Log.FirstEntryIndex,
        lastLogEntryIndex: view.Log.LastLogEntryIndex,
        commitIndex: view.Log.CommitIndex,
        queueSize: queueSize(view),
        criticalError: view.Log.CriticalError,
        installingSnapshot: isInstallingSnapshot(view),
        chocked: isChoked(view, referenceTimeUtc),
        progress: progress(view),
        connections: connections(view),
        installationLog: installationLog(view),
    };
}

function isFollower(view: Raven.Server.Rachis.RaftDebugView): view is Raven.Server.Rachis.FollowerDebugView {
    return view.Role === "Follower";
}

function isLeader(view: Raven.Server.Rachis.RaftDebugView): view is Raven.Server.Rachis.LeaderDebugView {
    return view.Role === "Leader";
}

function installationLog(view: Raven.Server.Rachis.RaftDebugView): Raven.Server.Rachis.RachisDebugMessage[] {
    if (isFollower(view) && isInstallingSnapshot(view)) {
        return view.RecentMessages;
    }

    return [];
}

function connections(view: Raven.Server.Rachis.RaftDebugView): Raven.Server.Rachis.RaftDebugView.PeerConnection[] {
    if (isFollower(view)) {
        return [view.ConnectionToLeader];
    }
    if (isLeader(view)) {
        return view.ConnectionToPeers;
    }

    return [];
}

function isInstallingSnapshot(view: Raven.Server.Rachis.RaftDebugView) {
    if (isFollower(view)) {
        return view.Phase === "Snapshot";
    }
    return false;
}

function progress(view: Raven.Server.Rachis.RaftDebugView) {
    const first = view.Log.FirstEntryIndex;
    const last = view.Log.LastLogEntryIndex;

    if (!first && !last) {
        return 100;
    }

    const logLength = last - first + 1;
    const queueLength = queueSize(view);

    return Math.ceil((100 * (logLength - queueLength)) / logLength);
}

function isChoked(view: Raven.Server.Rachis.RaftDebugView, referenceTimeUtc: moment.Moment) {
    const queueSizeCheck = queueSize(view) >= 5;

    const lastCommit = moment.utc(view.Log.LastCommitedTime);
    const lastCommitAgoInMs = referenceTimeUtc.diff(lastCommit);
    const lastCommitCheck = lastCommitAgoInMs >= 2 * 60 * 1_000; // 2 minutes

    return queueSizeCheck && lastCommitCheck;
}

function queueSize(view: Raven.Server.Rachis.RaftDebugView) {
    const log = view.Log;
    if (log.Logs.length === 0) {
        return 0;
    }

    return log.LastLogEntryIndex - log.CommitIndex;
}
