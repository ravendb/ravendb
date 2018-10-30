/// <reference path="../../../../typings/tsd.d.ts" />

import generalUtils = require("common/generalUtils");

type knownDocumentFlags = "HasRevisions" | "Revision" | "HasAttachments" | "DeleteRevision" | "HasCounters" | "Artificial";

interface revisionCounter {
    name: string;
    value: number;
}

class documentMetadata {
    collection: string;
    ravenClrType: string;
    nonAuthoritativeInfo: boolean;
    id: string;
    tempIndexScore: number;
    flags: string;
    lastModified = ko.observable<string>();
    nonStandardProps: Array<keyof documentMetadataDto>;

    lastModifiedFullDate: KnockoutComputed<string>;
    lastModifiedInterval: KnockoutComputed<string>;

    expirationDateInterval: KnockoutComputed<string>;
    expirationDateFullDate: KnockoutComputed<string>;
    
    attachments = ko.observableArray<documentAttachmentDto>();
    counters = ko.observableArray<string>();
    revisionCounters = ko.observableArray<revisionCounter>();
    
    changeVector = ko.observable<string>();

    constructor(dto?: documentMetadataDto) {
        if (dto) {
            this.collection = dto['@collection'];
            this.flags = dto['@flags'];
            this.ravenClrType = dto['Raven-Clr-Type'];
            this.nonAuthoritativeInfo = dto['Non-Authoritative-Information'];
            this.id = dto['@id'];
            this.tempIndexScore = dto['Temp-Index-Score'];

            const dateFormat = generalUtils.dateFormat;
            this.lastModifiedFullDate = ko.pureComputed(() => {
                const lastModified = this.lastModified();
                if (lastModified) {
                    const lastModifiedMoment = moment(lastModified);
                    return lastModifiedMoment.utc().format(dateFormat) + "(UTC)";
                }
                return "";
            });
            this.lastModifiedInterval = ko.pureComputed(() => {
                const lastModified = this.lastModified();
                if (this.lastModified()) {
                    const fromDuration = generalUtils.formatDurationByDate(moment.utc(lastModified), true);
                    return `${fromDuration} ago`;
                }
                return "";
            });
            
            this.expirationDateFullDate = ko.pureComputed(() => {
                const expiration = dto["@expires"];
                if (expiration) {
                    const expirationMoment = moment(expiration);
                    return expirationMoment.utc().format(dateFormat) + "(UTC)";
                }
                return "";
            });
            
            this.expirationDateInterval = ko.pureComputed(() => {
                const expiration = dto["@expires"];
                if (expiration) {
                    const fromDuration = generalUtils.formatDurationByDate(moment.utc(expiration), false);
                    return `in ${fromDuration}`;
                }
                return "";
            });

            this.lastModified(dto['@last-modified']);

            this.attachments(dto['@attachments']);
            
            this.counters(dto['@counters']);
            
            const revisionCounter = dto['@counters-snapshot'];
            this.revisionCounters(revisionCounter ? _.map(revisionCounter, (v, k) => ({ name: k, value: v })): []);

            this.changeVector(dto['@change-vector']);

            for (let property in dto) {
                if (property.toUpperCase() !== '@collection'.toUpperCase() &&
                    property.toUpperCase() !== '@flags'.toUpperCase() &&
                    property.toUpperCase() !== 'Raven-Clr-Type'.toUpperCase() &&
                    property.toUpperCase() !== 'Non-Authoritative-Information'.toUpperCase() &&
                    property.toUpperCase() !== '@id'.toUpperCase() &&
                    property.toUpperCase() !== 'Temp-Index-Score'.toUpperCase() &&
                    property.toUpperCase() !== '@last-modified'.toUpperCase() &&
                    property.toUpperCase() !== '@attachments'.toUpperCase() &&
                    property.toUpperCase() !== '@counters'.toUpperCase() &&
                    property.toUpperCase() !== '@counters-snapshot'.toUpperCase() &&
                    property.toUpperCase() !== 'toDto'.toUpperCase() &&
                    property.toUpperCase() !== '@change-vector'.toUpperCase()) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    (<any>this)[property] = (<any>dto)[property];
                    this.nonStandardProps.push(property as any);
                }
            }
        }
    }

    hasFlag(flag: knownDocumentFlags) {
        return this.flags ? _.includes(this.flags.split(", "), flag) : false;
    }

    toDto(): documentMetadataDto {
        const dto: documentMetadataDto = {
            '@collection': this.collection,
            'Raven-Clr-Type': this.ravenClrType,
            '@flags': this.flags,
            'Non-Authoritative-Information': this.nonAuthoritativeInfo,
            '@id': this.id,
            'Temp-Index-Score': this.tempIndexScore,
            '@last-modified': this.lastModified(),
            '@attachments': this.attachments(),
            '@counters': this.counters(),
            '@change-vector': this.changeVector()
        };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => dto[p] = (<any>this)[p]);
        }

        return dto;
    }

    static filterMetadata(metaDto: documentMetadataDto, removedProps: any[] = null, isClonedDocument: boolean = false) {
        // We don't want to show certain reserved properties in the metadata text area.
        // Remove them from the DTO, restore them on save.
        const metaPropsToRemove = ["@id", "@change-vector", "@last-modified", "@attachments", "@counters"];

        if (isClonedDocument) {
            metaPropsToRemove.push("@flags");
        }

        for (let property in metaDto) {
            if (metaDto.hasOwnProperty(property) && _.includes(metaPropsToRemove, property)) {
                if ((<any>metaDto)[property] && removedProps) {
                    removedProps.push({ name: property, value: (<any>metaDto)[property] });
                }
                delete (<any>metaDto)[property];
            }
        }
        return metaDto;
    }
    
    clearFlags() {
        this.flags = "";
        this.counters([]);
        this.attachments([]);
    }
}

export = documentMetadata;
