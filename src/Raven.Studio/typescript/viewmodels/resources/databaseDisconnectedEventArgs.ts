interface databaseDisconnectedEventArgs {
    databaseName: string;
    cause: databaseDisconnectionCause;
}

export = databaseDisconnectedEventArgs;
