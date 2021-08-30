/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import jsonUtil = require("common/jsonUtil");
import discoveryUrl = require("models/database/settings/discoveryUrl");

class connectionStringElasticSearchEtlModel extends connectionStringModel {

    //static serverWidePrefix = "Server Wide Raven Connection String";
    //isServerWide: KnockoutComputed<boolean>;

    //database = ko.observable<string>();
    elasticUrls = ko.observableArray<discoveryUrl>([]);
    inputUrl = ko.observable<discoveryUrl>(new discoveryUrl(""));
    selectedUrlToTest = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.Elasticsearch.ElasticsearchConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);

        this.update(dto);
        this.initValidation();

        const urlsCount = ko.pureComputed(() => this.elasticUrls().length);
        const urlsAreDirty = ko.pureComputed(() => {
            let anyDirty = false;

            this.elasticUrls().forEach(url => {
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

    update(dto: Raven.Client.Documents.Operations.ETL.Elasticsearch.ElasticsearchConnectionString) {
        super.update(dto);

        this.connectionStringName(dto.Name);
        this.elasticUrls(dto.Nodes.map((x) => new discoveryUrl(x)));
    }

    initValidation() {
        super.initValidation();

        this.elasticUrls.extend({
            validation: [
                {
                    validator: () => this.elasticUrls().length > 0,
                    message: "At least one Elastic Search destination url is required"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            topologyDiscoveryUrls: this.elasticUrls
        });
    }

    static empty(): connectionStringElasticSearchEtlModel {
        return new connectionStringElasticSearchEtlModel({
            Type: "Elasticsearch",
            Name: "",
            Nodes: []
        } as Raven.Client.Documents.Operations.ETL.Elasticsearch.ElasticsearchConnectionString, true, []);
    }

    toDto() {
        return {
            Type: "Elasticsearch",
            Name: this.connectionStringName(),
            // TopologyDiscoveryUrls: this.elasticUrls().map((x) => x.discoveryUrlName())
            Nodes: this.elasticUrls().map((x) => x.discoveryUrlName())
        };
    }

    // removeElasticUrl(url: discoveryUrl) {
    //     this.elasticUrls.remove(url);
    // }
    removeDiscoveryUrl(url: discoveryUrl) {
        this.elasticUrls.remove(url);
    }

    addDiscoveryUrlWithBlink() {
        if ( !_.find(this.elasticUrls(), x => x.discoveryUrlName() === this.inputUrl().discoveryUrlName())) {
            const newUrl = new discoveryUrl(this.inputUrl().discoveryUrlName());
            newUrl.dirtyFlag().forceDirty();
            this.elasticUrls.unshift(newUrl);
            this.inputUrl().discoveryUrlName("");

            $(".collection-list li").first().addClass("blink-style");
        }
    }

    testConnection(urlToTest: discoveryUrl) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        // return new testClusterNodeConnectionCommand(urlToTest.discoveryUrlName(), this.database(), false)
        return new testClusterNodeConnectionCommand(urlToTest.discoveryUrlName(), null, false) // todo ... we need new ep...
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
