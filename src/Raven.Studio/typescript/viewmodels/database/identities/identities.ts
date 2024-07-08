import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import getIdentitiesCommand = require("commands/database/identities/getIdentitiesCommand");
import seedIdentityCommand = require("commands/database/identities/seedIdentityCommand");
import getClientConfigurationCommand = require("commands/resources/getClientConfigurationCommand");
import getGlobalClientConfigurationCommand = require("commands/resources/getGlobalClientConfigurationCommand");
import genUtils = require("common/generalUtils");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database = require("models/resources/database");

class identity {
    prefix = ko.observable<string>();
    prefixWithPipe: KnockoutComputed<string>;
    prefixWithoutPipe: KnockoutComputed<string>;
    prefixAlreadyExists = ko.observable<boolean>(false);
    
    value = ko.observable<number>();
    currentValue = ko.observable<number>();
    warnAboutSmallerValue: KnockoutComputed<boolean>;
    
    textForEditIdentity: KnockoutComputed<string>;
    textForNewIdentity: KnockoutComputed<string>;
    
    identitySeparator = ko.observable<string>();
    static readonly defaultIdentitySeparator = "/";
    
    validationGroup = ko.validatedObservable();
    
    constructor(prefix: string, value: number, separator: string) {
        this.prefix(prefix);
        this.value(value);
        this.currentValue(value);
        this.identitySeparator(separator);
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables(): void {
        this.prefixWithPipe = ko.pureComputed(() => {
            let prefix = this.prefix();
            
            if (prefix && !prefix.endsWith("|")) {
                prefix += "|";
            }
            
            return prefix;
        });
        
        this.prefixWithoutPipe = ko.pureComputed(() => {
            const prefix = this.prefix();
            
            if (prefix.endsWith("|")) {
                return prefix.slice(0, -1);
            }

            return prefix;
        });
        
        this.textForEditIdentity = ko.pureComputed(() => {
            return `<ul class="margin-top">
                        <li>The next document that will be created using a <strong>pipe symbol</strong>, e.g.: <code>${genUtils.escapeHtml(this.prefixWithPipe())}</code>,
                            will have ID: <code>${genUtils.escapeHtml(this.prefixWithoutPipe())}${this.identitySeparator()}${this.value() + 1}</code>
                        </li>
                        <li class="margin-top-sm">${separatorText}</li>
                    </ul>`;
        });

        this.textForNewIdentity = ko.pureComputed(() => {
            return `<ul class="margin-top">
                        <li>When setting "Prefix" & "Value", the next document created using a <strong>pipe symbol</strong>, i.e.: <code>&lt;Prefix&gt;|</code><br/>
                            will be assigned an ID structured as <code>&lt;Prefix&gt;${genUtils.escapeHtml(this.identitySeparator())}&lt;Value + 1&gt;</code>
                        </li>
                        <li class="margin-top-sm">${separatorText}</li>
                    </ul>`;
        });
        
        const separatorText =
            `In the resulting ID, the Prefix and Value parts are separated by the effective separator character defined in your configuration,
            which is: <code>${genUtils.escapeHtml(this.identitySeparator())}</code>`;
        
        this.warnAboutSmallerValue = ko.pureComputed(() => {
           return this.value() < this.currentValue();
        });
    }

    private initValidation(): void {
        this.prefix.extend({
            required: true,
            validation: [
                {
                    validator: () => !this.prefixAlreadyExists(),
                    message: "Prefix already exists"
                }
            ]
        });

        this.value.extend({
            required: true,
            digit: true
        });
        
        this.validationGroup = ko.validatedObservable({
            name: this.prefix,
            value: this.value
        })
    }
    
    static empty(separator: string) {
        return new identity("", null, separator);
    }
}

class identities extends shardViewModelBase {
    
    view = require("views/database/identities/identities.html");

    editedIdentityItem = ko.observable<identity>(null);
    isNewIdentity = ko.observable<boolean>(false);
    
    filter = ko.observable<string>();

    identityPrefixList: string[] = [];
    
    private gridController = ko.observable<virtualGridController<identity>>();
    private columnPreview = new columnPreviewPlugin<identity>();

    clientVersion = viewModelBase.clientVersion;

    serverIdentitySeparator = ko.observable<string>();
    databaseIdentitySeparator = ko.observable<string>();
    effectiveIdentitySeparator: KnockoutComputed<string>;
    
    constructor(db: database) {
        super(db);
        
        this.bindToCurrentInstance("saveIdentity", "addNewIdentity", "cancel");
        this.initObservables();
    }

    private initObservables(): void {
        this.filter.throttle(500).subscribe(() => this.filterIdentities());
        
        this.effectiveIdentitySeparator = ko.pureComputed(() =>
            this.databaseIdentitySeparator() || this.serverIdentitySeparator() || identity.defaultIdentitySeparator)
    }
    
    private filterIdentities(): void {
        this.gridController().reset();
    }
    
    private fetchIdentitySeparator() {
        const serverClientConfigurationTask = new getGlobalClientConfigurationCommand()
            .execute()
            .done(dto => {
                const serverSeparator = dto ? (dto.Disabled ? null : (dto.IdentityPartsSeparator || identity.defaultIdentitySeparator)) : null;
                this.serverIdentitySeparator(serverSeparator);
             });

        const databaseClientConfigurationTask = new getClientConfigurationCommand(this.db)
            .execute()
            .done((dto) => {
                const databaseSeparator = dto ? (dto.Disabled ? null : (dto.IdentityPartsSeparator || identity.defaultIdentitySeparator)) : null;
                this.databaseIdentitySeparator(databaseSeparator);
             });

        return $.when<any>(serverClientConfigurationTask, databaseClientConfigurationTask);
    }

    private fetchIdentities(): JQueryPromise<pagedResult<identity>> {
        const task = $.Deferred<pagedResult<identity>>();
        
        this.fetchIdentitySeparator().then(() => {
            new getIdentitiesCommand(this.db)
                .execute()
                .done((identities: dictionary<number>) => {
                    const mappedIdentities = _.map(identities, (value, key): identity => {
                        return new identity(key, value, this.effectiveIdentitySeparator());
                    });

                    this.identityPrefixList = mappedIdentities.map(x => x.prefixWithoutPipe());

                    let filteredIdentities = this.filter() ?
                        mappedIdentities.filter(x => x.prefix().toLocaleLowerCase().includes(this.filter().toLocaleLowerCase())) :
                        mappedIdentities;

                    filteredIdentities = _.sortBy(filteredIdentities, x => x.prefix());

                    task.resolve({
                        totalResultCount: filteredIdentities.length,
                        items: filteredIdentities
                    });
                });
        });

        return task;
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);
       
        const prefixColumn = new textColumn<identity>(grid, x => x.prefix(), "Document ID Prefix", "60%", { sortable: x => x.prefix() });
        const valueColumn = new textColumn<identity>(grid, x => x.value().toLocaleString(), "Latest Value", "30%", { sortable: x => x.value() });
        const editColumn = new actionColumn<identity>(grid,
            x => this.editIdentity(x),
            "Edit",
            `<i class="icon-edit"></i>`,
            "10%",
            { title: () => 'Edit identity value', hide: () => this.isReadOnlyAccess() });

        const gridColumns = this.isReadOnlyAccess() ? [prefixColumn, valueColumn] : [prefixColumn, valueColumn, editColumn];
        
        grid.init(() => this.fetchIdentities(), () => gridColumns);
        
        this.columnPreview.install(".js-identities-grid", ".js-identities-tooltip",
            (identityItem: identity, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
            if (column instanceof textColumn) {
                onValue(column.getCellValue(identityItem));
            }
        });
    }

    addNewIdentity(): void {
        eventsCollector.default.reportEvent("identity", "new");

        this.fetchIdentitySeparator().then(() => {
            this.isNewIdentity(true);
            this.editedIdentityItem(identity.empty(this.effectiveIdentitySeparator()))

            this.editedIdentityItem().prefix.subscribe(() => {
                const item = this.editedIdentityItem();
                item.prefixAlreadyExists(this.identityPrefixList.find(x => x.toLocaleLowerCase() === item.prefixWithoutPipe().toLocaleLowerCase()) !== undefined);
            }); 
        });
    }
    
    editIdentity(identityItem: identity): void {
        this.fetchIdentitySeparator().then(() => {
            this.isNewIdentity(false);
            
            identityItem.identitySeparator(this.effectiveIdentitySeparator());
            this.editedIdentityItem(identityItem);
        });
    }
    
    cancel(): void {
        this.isNewIdentity(false);
        this.editedIdentityItem(null);
    }

    saveIdentity(): void {
        const item = this.editedIdentityItem();
        const prefix = item.prefixWithPipe();
        
        if (!this.isValid(item.validationGroup)) {
            return;
        }

        new seedIdentityCommand(this.db, prefix, item.value())
            .execute()
            .done(() => {
                this.editedIdentityItem(null);
                this.gridController().reset();
            });
    }
}

export = identities;
