import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import appUrl = require("common/appUrl");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 

class exportDatabase extends viewModelBase {
    batchSize = ko.observable(1024);
    exportActionUrl:KnockoutComputed<string>;
    noneDefualtFileName = ko.observable<string>("");
    chooseDifferntFileName = ko.observable<boolean>(false);
    authToken = ko.observable<string>();

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('N822WN');
        this.exportActionUrl = ko.computed(() => {
            var token = this.authToken();
            return appUrl.forResourceQuery(this.activeFilesystem()) + "/studio-tasks/exportFilesystem" + (token ? '?singleUseAuthToken=' + token : '');
        });
    }

    startExport() {
        var smugglerOptions = {
            BatchSize: this.batchSize(),
            NoneDefualtFileName: this.noneDefualtFileName()
        };
        
        $("#SmugglerOptions").val(JSON.stringify(smugglerOptions));

        new getSingleAuthTokenCommand(this.activeFilesystem()).execute().done((token: singleAuthToken) => {
            this.authToken(token.Token);
            $("#fsExportDownloadForm").submit();
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get Single Auth Token for export.", errorThrown));
    }
}

export = exportDatabase;
