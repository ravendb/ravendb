import viewModelBase = require("viewmodels/viewModelBase");
import migrateRavenDbDatabaseCommand = require("commands/database/studio/migrateRavenDbDatabaseCommand");
import migrateRavenDbDatabaseModel = require("models/database/tasks/migrateRavenDbDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import getMigratedServerUrlsCommand = require("commands/database/studio/getMigratedServerUrlsCommand");
import getRemoteServerVersionWithDatabasesCommand = require("commands/database/studio/getRemoteServerVersionWithDatabasesCommand");
import recentError = require("common/notifications/models/recentError");
import generalUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import defaultAceCompleter = require("common/defaultAceCompleter");

class migrateRavenDbDatabase extends viewModelBase {

    model = new migrateRavenDbDatabaseModel();
    completer = defaultAceCompleter.completer();

    spinners = {
        versionDetect: ko.observable<boolean>(false),
        getResourceNames: ko.observable<boolean>(false),
        migration: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("detectServerVersion");

        const debouncedDetection = _.debounce((showVersionSpinner: boolean) => this.detectServerVersion(showVersionSpinner), 700);

        this.model.serverUrl.subscribe(() => {
            this.model.serverMajorVersion(null);
            debouncedDetection(true);
        });

        this.model.userName.subscribe(() => debouncedDetection(false));
        this.model.password.subscribe(() => debouncedDetection(false));
        this.model.password.subscribe(() => debouncedDetection(false));
        this.model.apiKey.subscribe(() => debouncedDetection(false));
        this.model.enableBasicAuthenticationOverUnsecuredHttp.subscribe(() => debouncedDetection(false));
        this.model.skipServerCertificateValidation.subscribe(() => debouncedDetection(false));
    }

    attached() {
        super.attached();
        
        popoverUtils.longWithHover($("#configurationPopover"),
            {
                content:
                    "<div>The following configuration settings will be exported:</div>" +
                    "<strong>Revisions, Expiration & Client Configuration</strong>"
            });
    }
    
    activate(args: any) {
        super.activate(args);

        const deferred = $.Deferred<void>();
        new getMigratedServerUrlsCommand(this.activeDatabase())
            .execute()
            .done(data => this.model.serverUrls(data.List))
            .always(() => deferred.resolve());

        return deferred;
    }

    detectServerVersion(showVersionSpinner: boolean) {
        if (!this.isValid(this.model.versionCheckValidationGroup)) {
            this.model.serverMajorVersion(null);
            return;
        }

        this.spinners.getResourceNames(true);
        if (showVersionSpinner) {
            this.spinners.versionDetect(true);
        }

        const userName = this.model.showWindowsCredentialInputs() ? this.model.userName() : "";
        const password = this.model.showWindowsCredentialInputs() ? this.model.password() : "";
        const domain = this.model.showWindowsCredentialInputs() ? this.model.domain() : "";
        const apiKey = this.model.showApiKeyCredentialInputs() ? this.model.apiKey() : "";
        const enableBasicAuthenticationOverUnsecuredHttp = this.model.showApiKeyCredentialInputs() ? this.model.enableBasicAuthenticationOverUnsecuredHttp() : false;
        const skipServerCertificateValidation = this.model.skipServerCertificateValidation() ? this.model.skipServerCertificateValidation() : false;

        const url = this.model.serverUrl();
        new getRemoteServerVersionWithDatabasesCommand(url, userName, password, domain,
                apiKey, enableBasicAuthenticationOverUnsecuredHttp, skipServerCertificateValidation)
            .execute()
            .done(info => {
                if (info.MajorVersion !== "Unknown") {
                    this.model.serverMajorVersion(info.MajorVersion);
                    this.model.serverMajorVersion.clearError();
                    this.model.buildVersion(info.BuildVersion);
                    this.model.fullVersion(info.FullVersion);
                    this.model.productVersion(info.ProductVersion);
                    this.model.databaseNames(info.DatabaseNames);
                    this.model.fileSystemNames(info.FileSystemNames);
                    this.model.authorized(info.Authorized);
                    this.model.hasUnsecuredBasicAuthenticationOption(info.IsLegacyOAuthToken);
                    if (!info.Authorized) {
                        this.model.resourceName.valueHasMutated();
                    }
                } else {
                    this.model.serverMajorVersion(null);
                    this.model.buildVersion(null);
                    this.model.fullVersion(null);
                    this.model.productVersion(null);
                    this.model.databaseNames([]);
                    this.model.fileSystemNames([]);
                    this.model.authorized(true);
                    this.model.hasUnsecuredBasicAuthenticationOption(false);
                }
            })
            .fail((response: JQueryXHR) => {
                if (url === this.model.serverUrl()) {
                    const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                    const message = generalUtils.trimMessage(messageAndOptionalException.message);
                    this.model.serverMajorVersion.setError(message);
                    this.model.databaseNames([]);
                    this.model.fileSystemNames([]);
                }
            })
            .always(() => {
                this.spinners.getResourceNames(false);
                if (showVersionSpinner) {
                    this.spinners.versionDetect(false);
                }
            });
    }
    
    migrateDb() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("database", "migrate");
        this.spinners.migration(true);

        const db = this.activeDatabase();

        new migrateRavenDbDatabaseCommand(db, this.model)
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(db, operationId);
            })
            .always(() => this.spinners.migration(false));
    }
    compositionComplete() {
        super.compositionComplete();

        popoverUtils.longWithHover($("#scriptPopover"),
            {
                content:
                    "<div class=\"text-center\">Transform scripts are written in JavaScript </div>" +
                        "<pre><span class=\"token keyword\">var</span> name = <span class=\"token keyword\">this.</span>FirstName;<br />" +
                        "<span class=\"token keyword\">if</span> (name === <span class=\"token string\">'Bob'</span>)<br />&nbsp;&nbsp;&nbsp;&nbsp;" +
                        "<span class=\"token keyword\">throw </span><span class=\"token string\">'skip'</span>; <span class=\"token comment\">// filter-out</span><br /><br />" +
                        "<span class=\"token keyword\">this</span>.Freight = <span class=\"token number\">15.3</span>;<br />" +
                        "</pre>"
            });
    }
}

export = migrateRavenDbDatabase; 
