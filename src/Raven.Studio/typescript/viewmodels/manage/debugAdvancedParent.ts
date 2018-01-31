import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import durandalRouter = require("plugins/router");

class debugAdvanced extends viewModelBase {
    router: DurandalRootRouter;

    constructor() {
        super();

        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'admin/settings/debug/advanced/threadsRuntime',
                    moduleId: 'viewmodels/manage/debugAdvancedThreadsRuntime',
                    title: 'Threads Runtime Info',
                    tabName: "Threads Runtime Info",
                    nav: true,
                    hash: appUrl.forDebugAdvancedThreadsRuntime()
                },
                {
                    route: 'admin/settings/debug/advanced/memoryMappedFiles',
                    moduleId: 'viewmodels/manage/debugAdvancedMemoryMappedFiles',
                    title: 'Memory Mapped Files',
                    tabName: "Memory Mapped Files",
                    nav: true,
                    hash: appUrl.forDebugAdvancedMemoryMappedFiles()
                },
                {
                    route: 'admin/settings/debug/advanced/observerLog',
                    moduleId: 'viewmodels/manage/debugAdvancedObserverLog',
                    title: 'Cluster Observer Log',
                    tabName: "Cluster Observer Log",
                    nav: true,
                    hash: appUrl.forDebugAdvancedObserverLog(),                  
                }
            ])
            .buildNavigationModel();
    }
}

export = debugAdvanced; 
