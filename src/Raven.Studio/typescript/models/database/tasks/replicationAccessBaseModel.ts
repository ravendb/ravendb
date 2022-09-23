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
      
        this.samePrefixesForBothDirections(_.isEqual(hubToSink.map(x => x.path()), sinkToHub.map(x => x.path())));
       
        this.filteringPathsRequired(filteringPathsRequired);
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
                    validator: () => !this.filteringPathsRequired() || this.hubToSinkPrefixes().length,
                    message: "Please add at least one filtering path"
                }
            ]
        })

        this.sinkToHubPrefixes.extend({
            validation: [
                {
                    validator: () => !this.filteringPathsRequired() || this.samePrefixesForBothDirections() || this.sinkToHubPrefixes().length,
                    message: "Please add at least one filtering path, or use the Hub to Sink paths"
                }
            ]
        })
    }
    
    private hasSingleDocumentPattern(paths: prefixPathModel[]): KnockoutComputed<boolean> {
        return ko.pureComputed(() => paths.length && !!paths.find(x => !x.path().endsWith("*")));
    }
    
    getSingleDocumentPatternWarning() {
        return "Path patterns that do Not end with * (asterisk) will match only a single document";
    }

    addHubToSinkInputPrefixWithBlink() {
        const pathToAdd = this.inputPrefixHubToSink().path();
        
        if (!this.hubToSinkPrefixes().find(prefix => prefix.path() === pathToAdd))
        { 
            const itemToAdd = new prefixPathModel(pathToAdd);
            this.hubToSinkPrefixes.unshift(itemToAdd);

            this.inputPrefixHubToSink().path(null);
            $("#hubToSink .collection-list li").first().addClass("blink-style");
        }
    }

    addSinkToHubInputPrefixWithBlink() {
        const pathToAdd = this.inputPrefixSinkToHub().path();
        
        if (!this.sinkToHubPrefixes().find(prefix => prefix.path() === pathToAdd)) {
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
            AllowedHubToSinkPaths: this.hubToSinkPrefixes().map(prefix => prefix.path()),
            AllowedSinkToHubPaths: this.sinkToHubPrefixes().map(prefix => prefix.path())
        }
    }
}

export = replicationAccessBaseModel;
