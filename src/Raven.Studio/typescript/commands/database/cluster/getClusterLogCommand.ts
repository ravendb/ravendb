import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");


const follower: any = {
    "@metadata": {
        "DateTime": "2024-07-29T13:34:20.9387657Z",
        "WebUrl": "http://localhost:8080",
        "NodeTag": "A"
    },
    "Role": "Follower",
    "Term": 70,
    "CommandsVersion": {
        "Cluster": 60001,
        "Local": 60001
    },
    "Since": "2024-07-29T13:31:03.8925548Z",
    "Log": {
        "LastAppendedTime": "2024-07-29T13:33:06.1266622Z",
        "LastCommitedTime": "2024-07-29T13:33:06.1307309Z",
        "CommitIndex": 312,
        "LastTruncatedIndex": 306,
        "LastTruncatedTerm": 70,
        "FirstEntryIndex": 307,
        "LastLogEntryIndex": 312,
        "Logs": [
            {
                "Term": 70,
                "Index": 307,
                "SizeInBytes": 1636,
                "CommandType": "AddDatabaseCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:11.8203682",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 308,
                "SizeInBytes": 653,
                "CommandType": "UpdateTopologyCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:17.2207812",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 309,
                "SizeInBytes": 1633,
                "CommandType": "AddDatabaseCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:18.6223763",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 310,
                "SizeInBytes": 1631,
                "CommandType": "AddDatabaseCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:22.1238894",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 311,
                "SizeInBytes": 272,
                "CommandType": "PutSubscriptionCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:43.3392109",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 312,
                "SizeInBytes": 9011,
                "CommandType": "UpdateTopologyCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:33:06.1306897",
                "Entry": null
            }
        ]
    },
    "Phase": "Steady",
    "ConnectionToLeader": {
        "Destination": "A",
        "Version": 54000,
        "Compression": true,
        "Features": {
            "BaseLine": true,
            "MultiTree": true
        },
        "StartAt": "2024-07-29T13:31:03.8643341Z",
        "LastSent": "2024-07-29T13:34:20.8556324Z",
        "LastReceived": "2024-07-29T13:34:20.8554561Z"
    },
    "RecentMessages": [
        {
            "At": "2024-07-29T13:34:20.8558636Z",
            "MsFromCycleStart": 0,
            "Message": "Wait for entries"
        },
        {
            "At": "2024-07-29T13:34:20.8557712Z",
            "MsFromCycleStart": 0,
            "Message": "Start"
        }
    ]
}

const cIsDown: any = {
    "@metadata": {
        "DateTime": "2024-07-29T13:33:20.6308544Z",
        "WebUrl": "http://localhost:8081",
        "NodeTag": "B"
    },
    "Role": "Leader",
    "Term": 70,
    "CommandsVersion": {
        "Cluster": 60001,
        "Local": 60001
    },
    "Since": "2024-07-29T13:31:03.8849811Z",
    "Log": {
        "LastAppendedTime": "2024-07-29T13:33:06.1249923Z",
        "LastCommitedTime": "2024-07-29T13:33:06.1285614Z",
        "CommitIndex": 312,
        "LastTruncatedIndex": 306,
        "LastTruncatedTerm": 70,
        "FirstEntryIndex": 307,
        "LastLogEntryIndex": 312,
        "Logs": [
            {
                "Term": 70,
                "Index": 307,
                "SizeInBytes": 1636,
                "CommandType": "AddDatabaseCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:11.8152097",
                "Entry": null as any
            },
            {
                "Term": 70,
                "Index": 308,
                "SizeInBytes": 653,
                "CommandType": "UpdateTopologyCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:17.2191262",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 309,
                "SizeInBytes": 1633,
                "CommandType": "AddDatabaseCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:18.6211729",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 310,
                "SizeInBytes": 1631,
                "CommandType": "AddDatabaseCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:22.1227961",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 311,
                "SizeInBytes": 272,
                "CommandType": "PutSubscriptionCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:32:43.3370464",
                "Entry": null
            },
            {
                "Term": 70,
                "Index": 312,
                "SizeInBytes": 9011,
                "CommandType": "UpdateTopologyCommand",
                "Flags": "StateMachineCommand",
                "CreateAt": "2024-07-29T13:33:06.1285158",
                "Entry": null
            }
        ]
    },
    "ElectionReason": "Was elected by 2 nodes for leadership in term 70 with cluster version of 60001",
    "ConnectionToPeers": [
        {
            "Destination": "C",
            "Version": 54000,
            "Compression": true,
            "Features": {
                "BaseLine": true,
                "MultiTree": true
            },
            "StartAt": "2024-07-29T13:31:48.1517541Z",
            "LastSent": "2024-07-29T13:32:06.0484534Z",
            "LastReceived": "2024-07-29T13:32:05.9377815Z"
        },
        {
            "Destination": "A",
            "Version": 54000,
            "Compression": true,
            "Features": {
                "BaseLine": true,
                "MultiTree": true
            },
            "StartAt": "2024-07-29T13:31:03.8647324Z",
            "LastSent": "2024-07-29T13:33:20.5548203Z",
            "LastReceived": "2024-07-29T13:33:20.5555485Z"
        }
    ]
};

const installingSnapshot = {
    "@metadata": {
        "DateTime": "2024-07-29T12:42:30.2510543Z",
        "WebUrl": "http://localhost:8082",
        "NodeTag": "C"
    },
    "Role": "Follower",
    "Term": 66,
    "CommandsVersion": {
        "Cluster": 60001,
        "Local": 60001
    },
    "Since": "2024-07-29T12:42:19.0440781Z",
    "Log": {
        "LastAppendedTime": "2024-07-29T12:42:17.1975062Z",
        "LastCommitedTime": "2024-07-29T12:41:44.9496098Z",
        "CommitIndex": 261,
        "LastTruncatedIndex": 261,
        "LastTruncatedTerm": 66,
        "FirstEntryIndex": 0,
        "LastLogEntryIndex": 0,
        "Logs": [] as any[]
    },
    "Phase": "Snapshot",
    "ConnectionToLeader": {
        "Destination": "C",
        "Version": 54000,
        "Compression": true,
        "Features": {
            "BaseLine": true,
            "MultiTree": true
        },
        "StartAt": "2024-07-29T12:42:19.0428240Z",
        "LastSent": "2024-07-29T12:42:29.7801328Z",
        "LastReceived": "2024-07-29T12:42:19.0672724Z"
    },
    "RecentMessages": [
        {
            "At": "2024-07-29T12:42:30.2481910Z",
            "MsFromCycleStart": 11182,
            "Message": "Install LogHistory"
        },
        {
            "At": "2024-07-29T12:42:29.7801083Z",
            "MsFromCycleStart": 10714,
            "Message": "Install Items"
        },
        {
            "At": "2024-07-29T12:42:29.0337285Z",
            "MsFromCycleStart": 9967,
            "Message": "Install Identities"
        },
        {
            "At": "2024-07-29T12:42:28.5027526Z",
            "MsFromCycleStart": 9436,
            "Message": "Install CompareExchangeByExpiration"
        },
        {
            "At": "2024-07-29T12:42:23.7632064Z",
            "MsFromCycleStart": 4697,
            "Message": "Install CompareExchange"
        },
        {
            "At": "2024-07-29T12:42:22.5030555Z",
            "MsFromCycleStart": 3436,
            "Message": "Install CmpXchgTombstones"
        },
        {
            "At": "2024-07-29T12:42:21.0258989Z",
            "MsFromCycleStart": 1959,
            "Message": "Install CertificatesSlice"
        },
        {
            "At": "2024-07-29T12:42:19.0707191Z",
            "MsFromCycleStart": 4,
            "Message": "Start applying the snapshot"
        },
        {
            "At": "2024-07-29T12:42:19.0707188Z",
            "MsFromCycleStart": 4,
            "Message": "Finished reading the snapshot from stream with total size of 29.28 KBytes"
        },
        {
            "At": "2024-07-29T12:42:19.0706892Z",
            "MsFromCycleStart": 4,
            "Message": "Consumed 28.48 KBytes of the snapshot"
        },
        {
            "At": "2024-07-29T12:42:19.0704403Z",
            "MsFromCycleStart": 4,
            "Message": "Consumed 14.87 KBytes of the snapshot"
        },
        {
            "At": "2024-07-29T12:42:19.0680723Z",
            "MsFromCycleStart": 2,
            "Message": "Start receiving the snapshot"
        },
        {
            "At": "2024-07-29T12:42:19.0678877Z",
            "MsFromCycleStart": 1,
            "Message": "Got snapshot info: last included index:265 at term 66"
        },
        {
            "At": "2024-07-29T12:42:19.0674331Z",
            "MsFromCycleStart": 1,
            "Message": "Matching Negotiation is over, waiting for snapshot"
        },
        {
            "At": "2024-07-29T12:42:19.0658684Z",
            "MsFromCycleStart": 0,
            "Message": "Start"
        }
    ]
}

class getClusterLogCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Rachis.RaftDebugView> {
        const url = endpoints.global.rachisAdmin.adminClusterLog;

        return this.query<Raven.Server.Rachis.RaftDebugView>(url, null)
           // .then(x => follower) //TODO:  
            .fail((response: JQueryXHR) => this.reportError("Unable to get cluster log", response.responseText, response.statusText));
    }
}

export = getClusterLogCommand;
