import router = require("plugins/router");
import appUrl = require("common/appUrl");
import indexDefinition = require("models/indexDefinition");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");

class query extends viewModelBase {

    selectedIndex = ko.observable<string>();
    indexNames = ko.observableArray<string>();
    editIndexUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;
    statsUrl: KnockoutComputed<string>;
    hasSelectedIndex: KnockoutComputed<boolean>;
    queryText = ko.observable("");

    static containerSelector = "#queryContainer";

    constructor() {
        super();

        this.editIndexUrl = ko.computed(() => this.selectedIndex() ? appUrl.forEditIndex(this.selectedIndex(), this.activeDatabase()) : null);
        this.termsUrl = ko.computed(() => this.selectedIndex() ? appUrl.forTerms(this.selectedIndex(), this.activeDatabase()) : null);
        this.statsUrl = ko.computed(() => appUrl.forStatus(this.activeDatabase()));
        this.hasSelectedIndex = ko.computed(() => this.selectedIndex() != null);

        aceEditorBindingHandler.install();
    }

    activate(indexToSelect?: string) {
        super.activate(indexToSelect);

        this.fetchAllIndexes(indexToSelect);
    }

    attached() {
        this.useBootstrapTooltips();
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        $("#indexQueryLabel").popover({
            html: true,
            trigger: 'hover',
            container: '#indexQueryLabelContainer',
            content: 'Queries use Lucene syntax. Examples:<pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>',
        });
    }

    deactivate() {
        super.deactivate();
        this.removeKeyboardShortcuts(query.containerSelector);
    }

    editSelectedIndex() {
        router.navigate(this.editIndexUrl());
    }

    fetchAllIndexes(indexToSelect?: string) {
        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => {
                this.indexNames(stats.Indexes.map(i => i.PublicName));
                this.selectedIndex(indexToSelect || this.indexNames().first());
            });
    }
}

export = query;