import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import getDebugMemoryStatsCommand = require("commands/database/debug/getDebugMemoryStatsCommand");

type memoryMappingItem = {
    Directory: string;
    FileName: string;
    FileSize: number;
    TotalMapped: number;
    Mappings: Raven.Server.Documents.Handlers.Debugging.MemoryStatsHandler.MemoryInfoMappingDetails[];
}

class memoryMappedFiles extends viewModelBase {

    data = ko.observable<memoryMappingItem[]>();
    
    private gridController = ko.observable<virtualGridController<memoryMappingItem>>();
    columnsSelector = new columnsSelector<memoryMappingItem>();
    private columnPreview = new columnPreviewPlugin<memoryMappingItem>();
    
    spinners = {
        refresh: ko.observable<boolean>(false),
    };
    
    activate(args: any) {
        super.activate(args);
        
        return this.loadData();
    }

    compositionComplete(): void {
        super.compositionComplete();
        
        const fetcher = () => {
            const data = this.data() || [];
            
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
                new textColumn<memoryMappingItem>(grid, x => x.FileSize, "File Size", "15%"),
                new textColumn<memoryMappingItem>(grid, x => x.TotalMapped, "Total Mapped", "30%"),
            ]
        );

        this.columnPreview.install("virtual-grid", ".tooltip",
            (entry: memoryMappingItem, 
                                    column: textColumn<memoryMappingItem>, 
                                    e: JQueryEventObject, onValue: (context: any) => void) => {
            if (column.header === "Total Mapped") {
                const json = JSON.stringify(entry.Mappings, null, 4);
                const html = Prism.highlight(json, (Prism.languages as any).javascript);
                onValue(html);
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
                    return _.map(m.Details, (details: Raven.Server.Documents.Handlers.Debugging.MemoryStatsHandler.MemoryInfoMappingFileInfo, fileName: string) => {
                        return {
                            Directory: m.Directory,
                            FileName: fileName,
                            FileSize: details.FileSize,
                            TotalMapped: details.TotalMapped,
                            Mappings: details.Mappings
                        } as memoryMappingItem;
                    })
                });
                
                this.data(mappedResults);
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
