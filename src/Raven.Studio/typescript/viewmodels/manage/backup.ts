import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import backupDatabaseCommand = require("commands/maintenance/backupDatabaseCommand");
import getResourceDrives = require("commands/resources/getResourceDrives");
import accessHelper = require("viewmodels/shell/accessHelper");
import databasesManager = require("common/shell/databasesManager");

class resourceBackup {
    databasesManager = databasesManager.default;

    incremental = ko.observable<boolean>(false);
    resourceName = ko.observable<string>('');
    backupLocation = ko.observable<string>('');
    backupStatusMessages = ko.observableArray<backupMessageDto>();
    isBusy = ko.observable<boolean>(); 
    databaseNames: KnockoutComputed<string[]>;
    fullTypeName: KnockoutComputed<string>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;
    keepDown = ko.observable<boolean>(false);
    
    drivesForCurrentResource = ko.observable<string[]>([]);
    displaySameDriveWarning = ko.computed(() => {
        var resourceDrives = this.drivesForCurrentResource();
        var location = this.backupLocation().toLowerCase().substr(0, 3);
        return resourceDrives.indexOf(location) !== -1;
    }); 

    constructor(private qualifier: string, private resources: KnockoutObservableArray<database>) {
        this.databaseNames = ko.computed(() => resources().map((rs: database) => rs.name));

        this.fullTypeName = ko.computed(() => {
            var rs = resources();

            if (rs.length > 0) {
                return rs[0].fullTypeName;
            }
            return null;
        });

        this.searchResults = ko.computed(() => {
            var newDatabaseName = this.resourceName();
            return this.databaseNames().filter((name) => name.toLowerCase().indexOf(newDatabaseName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var newDatabaseName = this.resourceName();
            var foundDb = this.resources().find((rs: database) => newDatabaseName === rs.name && rs.qualifier === this.qualifier);

            if (!foundDb && newDatabaseName.length > 0) {
                errorMessage = this.fullTypeName() + " name doesn't exist!";
            }

            return errorMessage;
        });

        this.resourceName.throttle(200).subscribe((resource) => {
            var foundRs = this.resources().find((rs: database) => resource === rs.name && rs.qualifier === this.qualifier);
            if (foundRs) {
                new getResourceDrives(foundRs.name, foundRs.type.toString()).execute()
                    .done((drives: string[]) => {
                        this.drivesForCurrentResource(drives);
                    });
            } else {
                this.drivesForCurrentResource([]);
            }
        });
    }

    toggleKeepDown() {

        this.keepDown.toggle();
        this.forceKeepDown();
    }

    forceKeepDown() {
        if (this.keepDown()) {
            var body = document.getElementsByTagName("body")[0];
            body.scrollTop = body.scrollHeight;
        }
    }

    updateBackupStatus(newBackupStatus: backupStatusDto) {
        this.backupStatusMessages(newBackupStatus.Messages);
        this.isBusy(newBackupStatus.IsRunning);
        this.forceKeepDown();
    }
}

class backupDatabase extends viewModelBase {

    databasesManager = databasesManager.default;

    private dbBackupOptions = new resourceBackup(database.qualifier, this.databasesManager.databases);
    
    isForbidden = ko.observable<boolean>();

    canActivate(args: any): any {
        this.isForbidden(!accessHelper.isGlobalAdmin());
        return true;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("FT7RV6");
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which !== 13);
    }

    startDbBackup() {
        var backupOptions = this.dbBackupOptions;
        backupOptions.isBusy(true);

        const dbTobackup = this.databasesManager.getDatabaseByName(backupOptions.resourceName());
        new backupDatabaseCommand(dbTobackup, backupOptions.backupLocation(), backupOptions.updateBackupStatus.bind(this.dbBackupOptions), backupOptions.incremental())
            .execute()
            .always(() => backupOptions.isBusy(false));
    }
}

export = backupDatabase;
