/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import genUtils = require("common/generalUtils");

type configurationOrigin = "Default" | "Server" | "Database";

export abstract class settingsEntry {

    data: Raven.Server.Config.ConfigurationEntryDatabaseValue;

    keyName = ko.observable<string>();
    showEntry = ko.observable<boolean>();

    isServerWideOnlyEntry = ko.observable<boolean>();
    serverOrDefaultValue: KnockoutComputed<string>;
    hasServerValue: KnockoutComputed<boolean>;

    effectiveValue: KnockoutComputed<string>;
    effectiveValueOrigin: KnockoutComputed<configurationOrigin>;

    descriptionHtml: KnockoutComputed<string>;

    protected constructor(data: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        this.data = data;

        this.keyName(this.data.Metadata.Keys[this.data.Metadata.Keys.length - 1]);

        this.serverOrDefaultValue = ko.pureComputed(() => _.isEmpty(this.data.ServerValues) ? this.data.Metadata.DefaultValue : this.data.ServerValues[this.keyName()].Value);
        this.hasServerValue = ko.pureComputed(() => !_.isEmpty(this.data.ServerValues));
        
        this.descriptionHtml = ko.pureComputed(() => {
            const rawDescription = data.Metadata.Description;
            return rawDescription ? 
                `<div><span>${genUtils.escapeHtml(rawDescription)}</span></div>` :
                `<div><span class="text-muted">No description is available</span></div>`;
        });
    }

    static getEntry(rawEntry: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        if (rawEntry.Metadata.Scope === "ServerWideOnly") {
            return new serverWideOnlyEntry(rawEntry);
        }
        
        return databaseEntry.getEntry(rawEntry);
    }

    abstract getTemplateType(): settingsTemplateType;
}

export class serverWideOnlyEntry extends settingsEntry {

    constructor(data: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        super(data);
        this.isServerWideOnlyEntry(true);

        this.effectiveValue = ko.pureComputed(() => this.serverOrDefaultValue());
        this.effectiveValueOrigin = ko.pureComputed(() => this.hasServerValue() ? "Server" : "Default");
    }

    getTemplateType() {
        return "ServerWide" as settingsTemplateType;
    }
}

export abstract class databaseEntry<T> extends settingsEntry {
    customizedDatabaseValue = ko.observable<T>();
    override = ko.observable<boolean>();

    entryDirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutValidationGroup;

    static getEntry(rawEntry: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        let entry: databaseEntry<string | number>;
        
        switch (rawEntry.Metadata.Type) {
            case "String":
                entry = new stringEntry(rawEntry);
                break;
            case "Path":
                entry = new pathEntry(rawEntry);
                break;
            case "Integer":
                entry = new integerEntry(rawEntry);
                break;
            case "Double":
                entry = new doubleEntry(rawEntry);
                break;
            case "Boolean":
                entry = new booleanEntry(rawEntry);
                break;
            case "Enum":
                entry = new enumEntry(rawEntry);
                break;
            case "Time":
                entry = new timeEntry(rawEntry);
                break;
            case "Size":
                entry = new sizeEntry(rawEntry);
                break;
            default:
                throw new Error("Unknown entry type: " + rawEntry.Metadata.Type);
        }
        
        entry.init();
        return entry;
    }

    init() {
        this.isServerWideOnlyEntry(false);

        const hasDatabaseValues = !_.isEmpty(this.data.DatabaseValues); // if {key : value} or {key : null}
        this.override(hasDatabaseValues);

        if (hasDatabaseValues) {
            this.initCustomizedValue(this.data.DatabaseValues[this.keyName()].Value);
        } else {
            this.initCustomizedValue(this.serverOrDefaultValue());
        }

        this.effectiveValue = ko.pureComputed(() => {
            if (this.override()) {
                return this.getCustomizedValueAsString();
            }

            return this.serverOrDefaultValue();
        });

        this.effectiveValueOrigin = ko.pureComputed(() => this.override() ? "Database" : (this.hasServerValue() ? "Server" : "Default"));

        this.entryDirtyFlag = new ko.DirtyFlag([
            this.override,
            this.effectiveValue
        ], false, jsonUtil.newLineNormalizingHashFunction);

        this.initValidation();
    }

    getEntrySetting(): setttingsItem | null {
        if (!this.override()) {
            return null;
        }

        return { key: this.keyName(), value: this.effectiveValue() };
    }

    useDefaultValue() {
        this.initCustomizedValue(this.data.Metadata.DefaultValue);
    }

    abstract getCustomizedValueAsString(): string;
    abstract initCustomizedValue(value: string): void;
    abstract initValidation(): void;
}

export class stringEntry extends databaseEntry<string> {

    initCustomizedValue(value: string) {
        this.customizedDatabaseValue(value);
    }

    getCustomizedValueAsString(): string {
        return this.customizedDatabaseValue();
    }

    getTemplateType(): settingsTemplateType {
        return "String";
    }

    initValidation() {
        this.validationGroup = ko.validatedObservable({
            customizedDatabaseValue: this.customizedDatabaseValue
        });
    }
}

export class pathEntry extends databaseEntry<string> {
    folderPathOptions = ko.observableArray<string>([]);

    constructor(data: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        super(data);
        _.bindAll(this, "pathHasChanged");

        this.customizedDatabaseValue.throttle(300).subscribe((newPathValue) => {
            this.getFolderPathOptions(newPathValue);
        });
    }

    initCustomizedValue(value: string) {
        this.customizedDatabaseValue(value);
    }

    getCustomizedValueAsString(): string {
        return this.customizedDatabaseValue();
    }

    getTemplateType(): settingsTemplateType {
        return "Path";
    }

    initValidation() {
        this.customizedDatabaseValue.extend({
            required: {
                onlyIf: () => this.override() && !_.trim(this.customizedDatabaseValue())
            }
        });

        this.validationGroup = ko.validatedObservable({
            customizedDatabaseValue: this.customizedDatabaseValue
        });
    }

    pathHasChanged(pathValue: string) {
        this.customizedDatabaseValue(pathValue);
    }

    getFolderPathOptions(path?: string) {
        getFolderPathOptionsCommand.forServerLocal(path, true)
            .execute()
            .done((result: Raven.Server.Web.Studio.FolderPathOptions) => {
                this.folderPathOptions(result.List);
            });
    }
}

export abstract class numberEntry extends databaseEntry<number | null> {
    isNullable = ko.observable<boolean>(this.data.Metadata.IsNullable);
    minValue = ko.observable<number>(this.data.Metadata.MinValue);

    getCustomizedValueAsString(): string {
        if (this.isNullable() && !this.customizedDatabaseValue() && this.customizedDatabaseValue() !== 0) {
            return null;
        } // i.e. for Indexing.MapBatchSize to indicate 'no limit'

        const numberValue = this.customizedDatabaseValue();
        return numberValue >= this.minValue() ? numberValue.toString() : null;
        // must have null option returned here for the validation of empty template
    }

    initValidation() {
        this.customizedDatabaseValue.extend({
            required: {
                onlyIf: () => this.override() &&
                    !this.isNullable() &&
                    !this.customizedDatabaseValue()
            },
            validation: [
                {
                    validator: (value: number) => (value >= this.minValue() ||
                        (this.isNullable() && !value && value !== 0)),
                    message: "Please enter a value greater or equal to {0}",
                    params:  this.minValue()
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            customizedDatabaseValue: this.customizedDatabaseValue
        });
    }
}

export class integerEntry extends numberEntry {

    initCustomizedValue(value?: string) {
        const integerValue = value ? parseInt(value) : null;
        this.customizedDatabaseValue(integerValue);
    }

    getTemplateType(): settingsTemplateType {
        return "Integer";
    }

    initValidation() {
        super.initValidation();

        this.customizedDatabaseValue.extend({
            digit: true
        });
    }
}

export class doubleEntry extends numberEntry {

    initCustomizedValue(value?: string) : void {
        const doubleValue = value ? parseFloat(value) : null;
        this.customizedDatabaseValue(doubleValue);
    }

    getTemplateType(): settingsTemplateType {
        return "Double";
    }
}

export class sizeEntry extends numberEntry {
    sizeUnit = ko.observable<string>(this.data.Metadata.SizeUnit);

    initCustomizedValue(value?: string) {
        const sizeValue = value ? parseInt(value) : null;
        this.customizedDatabaseValue(sizeValue);
    }

    getTemplateType(): settingsTemplateType {
        return "Size";
    }

    initValidation() {
        super.initValidation();

        this.customizedDatabaseValue.extend({
            digit: true
        });
    }
}

export class timeEntry extends numberEntry {
    timeUnit = ko.observable<string>();
    specialTimeEntry = "Indexing.MapTimeoutInSec"; // Not nullable and default value is -1

    initCustomizedValue(value?: string) {
        const timeValue = value ? parseInt(value) : null;
        this.customizedDatabaseValue(timeValue);

        this.timeUnit(this.data.Metadata.TimeUnit);
        this.minValue(this.keyName() === this.specialTimeEntry ? -1 : this.data.Metadata.MinValue || 0);
    }

    getTemplateType(): settingsTemplateType {
        return "Time";
    }

    initValidation() {
        super.initValidation();

        this.customizedDatabaseValue.extend({
            digit: {
                onlyIf: () => this.keyName() !== this.specialTimeEntry || this.customizedDatabaseValue() !== -1
            }
        });
    }
}

export class enumEntry extends databaseEntry<string> {
    availableValues = ko.observableArray<string>(this.data.Metadata.AvailableValues);

    constructor(data: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        super(data);
        _.bindAll(this, "initCustomizedValue");
    }

    initCustomizedValue(value: string) {
        this.customizedDatabaseValue(value);
    }

    getCustomizedValueAsString(): string {
        return this.customizedDatabaseValue();
    }

    getTemplateType(): settingsTemplateType {
        return "Enum";
    }

    initValidation() {
        this.customizedDatabaseValue.extend({
            required: {
                onlyIf: () => this.override()
            }
        });

        this.validationGroup = ko.validatedObservable({
            customizedDatabaseValue: this.customizedDatabaseValue
        });
    }
}

export class booleanEntry extends enumEntry {

    constructor(data: Raven.Server.Config.ConfigurationEntryDatabaseValue) {
        super(data);
        this.availableValues(["True", "False"]);
    }
}
