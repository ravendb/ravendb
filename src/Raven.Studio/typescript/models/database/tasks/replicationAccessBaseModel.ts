/// <reference path="../../../../typings/tsd.d.ts"/>
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import prefixPathModel = require("models/database/tasks/prefixPathModel");
import jsonUtil = require("common/jsonUtil");

class replicationAccessBaseModel {
    replicationAccessName = ko.observable<string>();
    
    certificate = ko.observable<replicationCertificateModel>();
    
    inputPrefixHubToSink = ko.observable<prefixPathModel>(new prefixPathModel(null));
    inputPrefixSinkToHub = ko.observable<prefixPathModel>(new prefixPathModel(null));
    
    hubToSinkPrefixes = ko.observableArray<prefixPathModel>([]);
    sinkToHubPrefixes = ko.observableArray<prefixPathModel>([]);
    
    samePrefixesForBothDirections = ko.observable<boolean>(false);
    filteringPathsRequired = ko.observable<boolean>(true);
        
    dirtyFlag = new ko.DirtyFlag([]);

    constructor(accessName: string, certificate: replicationCertificateModel, hubToSink: prefixPathModel[], sinkToHub: prefixPathModel[], filteringPathsRequired = true) {
       
        this.replicationAccessName(accessName);
        this.hubToSinkPrefixes(hubToSink);
        this.sinkToHubPrefixes(sinkToHub);
        this.certificate(certificate);
      
        this.samePrefixesForBothDirections(_.isEqual(this.getNormalizedPrefixPaths(hubToSink), this.getNormalizedPrefixPaths(sinkToHub)));
       
        this.filteringPathsRequired(filteringPathsRequired);
    }

    private getNormalizedPrefixPaths(prefixes: prefixPathModel[]) {
        return _.compact(prefixes.map(x => prefixPathModel.normalize(x.path())));
    }

    getHubToSinkPrefixesToSave() {
        return this.getNormalizedPrefixPaths(this.hubToSinkPrefixes());
    }

    getSinkToHubPrefixesToSave() {
        return this.getNormalizedPrefixPaths(this.sinkToHubPrefixes());
    }
    
    initObservables() {
        this.dirtyFlag = new ko.DirtyFlag([
            this.replicationAccessName,
            this.certificate,
            this.hubToSinkPrefixes,
            this.sinkToHubPrefixes,
            this.samePrefixesForBothDirections
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    initValidation() {
        this.replicationAccessName.extend({
           required: true
        });
        
        this.certificate.extend({
            required: true
        })
        
        this.hubToSinkPrefixes.extend({
            validation: [
                {
                    validator: () => !this.filteringPathsRequired() || this.getHubToSinkPrefixesToSave().length,
                    message: "Please add at least one filtering path"
                }
            ]
        })

        this.sinkToHubPrefixes.extend({
            validation: [
                {
                    validator: () => !this.filteringPathsRequired() || this.samePrefixesForBothDirections() || this.getSinkToHubPrefixesToSave().length,
                    message: "Please add at least one filtering path, or use the Hub to Sink paths"
                }
            ]
        })
    }
    
    private hasSingleDocumentPattern(paths: prefixPathModel[]): KnockoutComputed<boolean> {
        return ko.pureComputed(() => paths.length && !!paths.find(x => {
            const normalizedPath = prefixPathModel.normalize(x.path());
            return normalizedPath && !normalizedPath.endsWith("*");
        }));
    }
    
    getSingleDocumentPatternWarning() {
        return "Path patterns that do not end with * (asterisk) will match only a single document";
    }

    addHubToSinkInputPrefixWithBlink() {
        const pathToAdd = prefixPathModel.normalize(this.inputPrefixHubToSink().path());

        if (!pathToAdd) {
            this.inputPrefixHubToSink().path(null);
            return;
        }
        
        if (!this.hubToSinkPrefixes().find(prefix => prefixPathModel.normalize(prefix.path()) === pathToAdd))
        { 
            const itemToAdd = new prefixPathModel(pathToAdd);
            this.hubToSinkPrefixes.unshift(itemToAdd);

            this.inputPrefixHubToSink().path(null);
            $("#hubToSink .collection-list li").first().addClass("blink-style");
        }
    }

    addSinkToHubInputPrefixWithBlink() {
        const pathToAdd = prefixPathModel.normalize(this.inputPrefixSinkToHub().path());

        if (!pathToAdd) {
            this.inputPrefixSinkToHub().path(null);
            return;
        }
        
        if (!this.sinkToHubPrefixes().find(prefix => prefixPathModel.normalize(prefix.path()) === pathToAdd)) {
            const itemToAdd = new prefixPathModel(pathToAdd);
            this.sinkToHubPrefixes.unshift(itemToAdd);

            this.inputPrefixSinkToHub().path(null);
            $("#sinkToHub .collection-list li").first().addClass("blink-style");
        }
    }
    
    removePrefixPathHubToSink(pathToRemove: string) {
        const itemToRemove = this.hubToSinkPrefixes().find(x => x.path() === pathToRemove);
        this.hubToSinkPrefixes.remove(itemToRemove);
    }
    
    removePrefixPathSinkToHub(pathToRemove: string) {
        const itemToRemove = this.sinkToHubPrefixes().find(x => x.path() === pathToRemove);
        this.sinkToHubPrefixes.remove(itemToRemove);
    }
    
    toDto(): Raven.Client.Documents.Operations.Replication.ReplicationHubAccess {
        return {
            Name: this.replicationAccessName(),
            CertificateBase64: this.certificate().publicKey(),
            AllowedHubToSinkPaths: this.getHubToSinkPrefixesToSave(),
            AllowedSinkToHubPaths: this.getSinkToHubPrefixesToSave()
        }
    }
}

export = replicationAccessBaseModel;
