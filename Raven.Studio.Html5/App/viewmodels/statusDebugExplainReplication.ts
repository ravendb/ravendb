import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import extensions = require("common/extensions");
import getReplicationsCommand = require('commands/getReplicationsCommand');
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import explainReplicationCommand = require("commands/explainReplicationCommand");

class statusDebugExplainReplication extends viewModelBase {

    destinations = ko.observable<replicationDestinationDto[]>();
    selectedDestination = ko.observable<replicationDestinationDto>();
    documentId = ko.observable<string>();
    documentIdSearchResults = ko.observableArray<string>();
    explanation = ko.observable<replicationExplanationForDocumentDto>();

    constructor() {
        super();
        extensions.install();
        this.documentId.throttle(250).subscribe(search => this.fetchDocSearchResults(search));
    }

    canActivate(args) {
        super.canActivate(args);

        var deferred = $.Deferred();

        $.when(this.fetchReplicationDestinations())
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ can: false }));

        return deferred;
    }

    fetchReplicationDestinations() {
        return new getReplicationsCommand(this.activeDatabase())
            .execute()
            .done((destinations: replicationsDto) => {
                this.destinations(destinations.Destinations);
            });
        return true;
    }

    buttonEnabled = ko.computed(() => {
        var destionationSelected = this.selectedDestination() != null;
        var documentSelected = this.documentId();
        return destionationSelected && (documentSelected != null && documentSelected.length > 0);
    });

    setSelectedDestination(destination: replicationDestinationDto) {
        this.selectedDestination(destination);
    }

    selectDocument(data: documentMetadataDto) {
        this.documentId(data['@metadata']['@id']);
    }

    fetchDocSearchResults(query: string) {
        if (query.length >= 2) {
            new getDocumentsMetadataByIDPrefixCommand(query, 10, this.activeDatabase())
                .execute()
                .done((results: string[]) => {
                    if (this.documentId() === query) {
                        this.documentIdSearchResults(results);
                    }
                });
        } else if (query.length == 0) {
            this.documentIdSearchResults.removeAll();
        }
    }

    explain() {
        new explainReplicationCommand(this.activeDatabase(), this.documentId(), this.selectedDestination().Url, this.selectedDestination().Database)
            .execute()
            .done(result => this.explanation(result)); 
    }

    selectedDestionationText = ko.computed(() => {
        var dest = this.selectedDestination();
        if (dest) {
            return dest.Database + ' on ' + dest.Url;
        } else {
            return 'select destination';
        }
    });
}

export = statusDebugExplainReplication;