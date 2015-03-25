import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import facet = require("models/facet");
import queryFacetsCommand = require("commands/queryFacetsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");

class reporting extends viewModelBase {
    selectedIndexName = ko.observable<string>();
    selectedIndexLabel = ko.computed(() => this.selectedIndexName() ? this.selectedIndexName() : "[Select an index]");
    indexNames = ko.observableArray<string>();
    hasSelectedIndex = ko.computed(() => this.selectedIndexName() && this.selectedIndexName().length > 0);
    editSelectedIndexUrl = ko.computed(() => this.hasSelectedIndex() ? appUrl.forEditIndex(this.selectedIndexName(), this.activeDatabase()) : null);
    availableFields = ko.observableArray<string>();
    sortOptions = ko.observableArray<any>();
    selectedField = ko.observable<string>();
    selectedFieldLabel = ko.computed(() => this.selectedField() ? this.selectedField() : "Select a field");
    addedValues = ko.observableArray<facet>();
    filter = ko.observable<string>();
    hasFilter = ko.observable(false);
    reportResults = ko.observable<pagedList>();
    totalQueryResults = ko.computed(() => this.reportResults() ? this.reportResults().totalResultCount() : null);
    queryDuration = ko.observable<string>();
	appUrls: computedAppUrls;
	isCacheDisable = ko.observable<boolean>(false);
    
    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
	}

	attached() {
		$("#filterQueryLabel").popover({
			html: true,
			trigger: "hover",
			container: ".form-horizontal",
			content: 'Queries use Lucene syntax. Examples:<pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>',
		});
	}

    activate(indexToActivateOrNull: string) {
        super.activate(indexToActivateOrNull);
        this.updateHelpLink('O3EA1R');

        this.fetchIndexes().done(() => this.selectInitialIndex(indexToActivateOrNull));
        this.selectedIndexName.subscribe(() => this.resetSelections());

        aceEditorBindingHandler.install();
    }

    fetchIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => this.indexNames(results.Indexes.map(i => i.Name)));
    }

    fetchIndexDefinition(indexName: string) {
        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((dto: indexDefinitionContainerDto) => {
                this.sortOptions(dto.Index.SortOptions);
                this.availableFields(dto.Index.Fields);
            });
    }

    selectInitialIndex(indexToActivateOrNull: string) {
        if (indexToActivateOrNull && this.indexNames.contains(indexToActivateOrNull)) {
            this.setSelectedIndex(indexToActivateOrNull);
        } else if (this.indexNames().length > 0) {
            this.setSelectedIndex(this.indexNames.first());
        }
    }

    setSelectedIndex(indexName: string) {
        this.selectedIndexName(indexName);
        this.updateUrl(appUrl.forReporting(this.activeDatabase(), indexName));

        this.fetchIndexDefinition(indexName);
    }

    setSelectedField(fieldName: string) {
        this.selectedField(fieldName);

        // Update all facets to use that too.
        this.addedValues().forEach(v => v.name = fieldName);
    }

    resetSelections() {
        this.selectedField(null);
        this.addedValues([]);
        this.availableFields([]);
    }

    mapSortToType(sort: string) {
        switch (sort) {
            case 'Int':
                return "System.Int32";
            case 'Float':
                return "System.Single";
            case 'Long':
                return 'System.Int64';
            case 'Double':
                return "System.Double";
            case 'Short':
                return 'System.Int16';
            case 'Byte':
                return 'System.Byte';
            default: 
                return 'System.String';
        }
    }

    addValue(fieldName: string) {
        var sortOps = this.sortOptions();
        var sortOption = (fieldName in sortOps) ? sortOps[fieldName] : "String";
        var val = facet.fromNameAndAggregation(this.selectedField(), fieldName, this.mapSortToType(sortOption));
        this.addedValues.push(val);
    }

    removeValue(val: facet) {
        this.addedValues.remove(val);
    }

    runReport() {
        var selectedIndex = this.selectedIndexName();
        var filterQuery = this.hasFilter() ? this.filter() : null;
        var facets = this.addedValues().map(v => v.toDto());
        var groupedFacets: facetDto[] = [];
        facets.forEach((curFacet) => {
            var foundFacet = groupedFacets.first(x => x.AggregationField == curFacet.AggregationField);

            if (foundFacet) {
                foundFacet.Aggregation += curFacet.Aggregation;
            } else {
                groupedFacets.push(curFacet);
            }

        });
        var db = this.activeDatabase();
        var resultsFetcher = (skip: number, take: number) => {
            return new queryFacetsCommand(selectedIndex, filterQuery, skip, take, groupedFacets, db, this.isCacheDisable())
                .execute()
                .done((resultSet: pagedResultSet) => this.queryDuration(resultSet.additionalResultInfo));
        };
        this.reportResults(new pagedList(resultsFetcher));
	}

	toggleCacheEnable() {
		this.isCacheDisable(!this.isCacheDisable());
	}
}

export = reporting;