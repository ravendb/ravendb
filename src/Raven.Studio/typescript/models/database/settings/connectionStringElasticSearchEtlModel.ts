/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import jsonUtil = require("common/jsonUtil");
import discoveryUrl = require("models/database/settings/discoveryUrl");

class connectionStringElasticSearchEtlModel extends connectionStringModel {
    nodesUrls = ko.observableArray<discoveryUrl>([]);
    inputUrl = ko.observable<discoveryUrl>(new discoveryUrl(""));
    selectedUrlToTest = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);

        this.update(dto);
        this.initValidation();

        const urlsCount = ko.pureComputed(() => this.nodesUrls().length);
        const urlsAreDirty = ko.pureComputed(() => {
            let anyDirty = false;

            this.nodesUrls().forEach(url => {
                if (url.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });

            return anyDirty;
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            urlsCount,
            urlsAreDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString) {
        super.update(dto);

        this.connectionStringName(dto.Name);
        this.nodesUrls(dto.Nodes.map((x) => new discoveryUrl(x)));
    }

    initValidation() {
        super.initValidation();

        this.nodesUrls.extend({
            validation: [
                {
                    validator: () => this.nodesUrls().length > 0,
                    message: "At least one Elastic Search destination url is required"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            topologyDiscoveryUrls: this.nodesUrls
        });
    }

    static empty(): connectionStringElasticSearchEtlModel {
        return new connectionStringElasticSearchEtlModel({
            Type: "ElasticSearch",
            Name: "",
            Nodes: []
        } as Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString, true, []);
    }

    toDto() {
        return {
            Type: "Elasticsearch",
            Name: this.connectionStringName(),
            Nodes: this.nodesUrls().map((x) => x.discoveryUrlName())
        };
    }

    // removeElasticUrl(url: discoveryUrl) {
    //     this.elasticUrls.remove(url);
    // }
    removeDiscoveryUrl(url: discoveryUrl) {
        this.nodesUrls.remove(url);
    }

    addDiscoveryUrlWithBlink() {
        if ( !_.find(this.nodesUrls(), x => x.discoveryUrlName() === this.inputUrl().discoveryUrlName())) {
            const newUrl = new discoveryUrl(this.inputUrl().discoveryUrlName());
            newUrl.dirtyFlag().forceDirty();
            this.nodesUrls.unshift(newUrl);
            this.inputUrl().discoveryUrlName("");

            $(".collection-list li").first().addClass("blink-style");
        }
    }

    testConnection(urlToTest: discoveryUrl) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        // return new testClusterNodeConnectionCommand(urlToTest.discoveryUrlName(), this.database(), false)
        return new testClusterNodeConnectionCommand(urlToTest.discoveryUrlName(), null, false) // TODO ... we need new ep...
            .execute()
            .done((result) => {
                if (result.Error) {
                    urlToTest.hasTestError(true);
                }
            });
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }
}

export = connectionStringElasticSearchEtlModel;
