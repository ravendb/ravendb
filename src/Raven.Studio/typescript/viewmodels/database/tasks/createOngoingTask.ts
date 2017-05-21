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

    newReplicationTask() {
        alert("NewReplicationTask");
        // ...
    }

    newEtlTask() {
        alert("NewEtlTask");
        // ...
    }

    newSqlEtlTask() {
        alert("NewSqlEtlTask");
        // ...
    }

    newBackupTask() {
        alert("NewBackupTask");
        // ...
    }

    newSubscriptionTask() {
        alert("NewSubscriptionTask");
        // ... 
    }
}

export = createOngoingTask;
