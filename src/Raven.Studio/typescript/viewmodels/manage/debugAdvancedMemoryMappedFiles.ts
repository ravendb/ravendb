import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import getDebugMemoryStatsCommand = require("commands/database/debug/getDebugMemoryStatsCommand");

type memoryMappingItem = {
    Directory: string;
    FileName: string;
    HumaneFileSize: string;
    HumaneTotalMapped: string;
    FileSize: number;
    TotalMapped: number;
    Mappings: Raven.Server.Documents.Handlers.Debugging.MemoryDebugHandler.MemoryInfoMappingDetails[];
}

class memoryMappedFiles extends viewModelBase {

    allData = ko.observable<memoryMappingItem[]>();
    filteredData = ko.observable<memoryMappingItem[]>();
    filter = ko.observable<string>();
    
    private gridController = ko.observable<virtualGridController<memoryMappingItem>>();
    private columnPreview = new columnPreviewPlugin<memoryMappingItem>();
    
    filesCount: KnockoutComputed<number>;
    totalSizeOnDisk: KnockoutComputed<string>;
    totalMappedSize: KnockoutComputed<string>;
    
    spinners = {
        refresh: ko.observable<boolean>(false),
    };
    
    constructor() {
        super();
        
        this.filesCount = ko.pureComputed(() => {
            return this.filteredData().length;
        });
        
        this.totalSizeOnDisk = ko.pureComputed(() => {
            return generalUtils.formatBytesToSize(_.sum(this.filteredData().map(x => x.FileSize))); 
        });
        
        this.totalMappedSize = ko.pureComputed(() => {
            return generalUtils.formatBytesToSize(_.sum(this.filteredData().map(x => x.TotalMapped)));
        });
        
        this.filter.throttle(500).subscribe(() => this.filterEntries());
    }
    
    private filterEntries() {
        if (this.gridController()) {
            const filter = this.filter();
            if (filter) {
                this.filteredData(this.allData().filter(item => this.matchesFilter(item)));
            } else {
                this.filteredData(this.allData().slice());
            }
    
            this.gridController().reset(true);    
        } else {
            this.filteredData(this.allData().slice());
        }
    }
    
    private matchesFilter(item: memoryMappingItem) {
        const filter = this.filter();
        if (!filter) {
            return true;
        }
        const filterLowered = filter.toLocaleLowerCase();
        return item.FileName.toLocaleLowerCase().includes(filterLowered) || item.Directory.toLocaleLowerCase().includes(filterLowered);
    }
    
    activate(args: any) {
        super.activate(args);
        
        return this.loadData();
    }

    compositionComplete(): void {
        super.compositionComplete();
        
        const fetcher = () => {
            const data = this.filteredData() || [];
            
            return $.when({
                totalResultCount: data.length,
                items: data
            } as pagedResult<memoryMappingItem>);
        };

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(fetcher, () =>
            [
                new textColumn<memoryMappingItem>(grid, x => x.Directory, "Directory", "15%"),
                new textColumn<memoryMappingItem>(grid, x => x.FileName, "File Name", "25%"),
                new textColumn<memoryMappingItem>(grid, x => x.HumaneFileSize, "File Size", "15%"),
                new textColumn<memoryMappingItem>(grid, x => x.HumaneTotalMapped, "Total Mapped", "30%"),
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-memory-mapped-files-tooltip",
            (entry: memoryMappingItem, 
                                    column: textColumn<memoryMappingItem>, 
                                    e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
            if (column.header === "Total Mapped") {
                const json = JSON.stringify(entry.Mappings, null, 4);
                const html = Prism.highlight(json, (Prism.languages as any).javascript);
                onValue(html, json);
            } else {
                const value = column.getCellValue(entry);
                onValue(value);
            }
        });
    }
    
    private loadData() {
        return new getDebugMemoryStatsCommand()
            .execute()
            .done(response => {
                
                const mappedResults = _.flatMap(response.Mappings, m => {
                    return _.map(m.Details, (details: Raven.Server.Documents.Handlers.Debugging.MemoryDebugHandler.MemoryInfoMappingFileInfo, fileName: string) => {
                        return {
                            Directory: m.Directory,
                            FileName: fileName,
                            HumaneFileSize: details.HumaneFileSize,
                            HumaneTotalMapped: details.HumaneTotalMapped,
                            FileSize: details.FileSize,
                            TotalMapped: details.TotalMapped,
                            Mappings: details.Mappings
                        } as memoryMappingItem;
                    })
                });
                
                this.allData(_.sortBy(mappedResults, x => x.Directory + x.FileName));
                this.filterEntries();
            });
    }
    
    refresh() {
        this.spinners.refresh(true);
        return this.loadData()
            .done(() => this.gridController().reset(true))
            .always(() => this.spinners.refresh(false));
    }
    
}

export = memoryMappedFiles;
