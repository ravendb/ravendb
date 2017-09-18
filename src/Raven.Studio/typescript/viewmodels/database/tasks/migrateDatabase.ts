import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");

class migrateDatabase extends viewModelBase {

    model = new migrateDatabaseModel();
    startingMigration = ko.observable<boolean>(false);

    attached() {
        super.attached();

        this.updateHelpLink("YD9M1R");
    }

    canDeactivate(isClose: boolean) {
        super.canDeactivate(isClose);

        if (this.startingMigration()) {
            this.confirmationMessage("Initiating migration", "Please wait until its completion.", ["OK"]);
            return false;
        }

        return true;
    }

    migrateDb() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("database", "migrate");
        this.startingMigration(true);

        const db = this.activeDatabase();

        new migrateDatabaseCommand(db, this.model)
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(db, operationId);
            })
            .always(() => this.startingMigration(false));
    }
}

export = migrateDatabase; 
