import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createOngoingTask extends dialogViewModelBase {

    constructor() {
        super();
    }

    activate() {
    }

    compositionComplete() {
        super.compositionComplete();
    }

    protected initObservables() {
    }

    NewReplicationTask() {
        alert("NewReplicationTask");
        // ...
    }

    NewEtlTask() {
        alert("NewEtlTask");
        // ...
    }

    NewSqlEtlTask() {
        alert("NewSqlEtlTask");
        // ...
    }

    NewBackupTask() {
        alert("NewBackupTask");
        // ...
    }

    NewSubscriptionTask() {
        alert("NewSubscriptionTask");
        // ... 
    }
}

export = createOngoingTask;
