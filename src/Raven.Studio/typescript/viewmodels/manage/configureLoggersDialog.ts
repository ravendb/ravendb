import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class loggerCategory {
    source = ko.observable<string>();
    logLevel = ko.observable<Sparrow.Logging.LogMode>("Operations");
    
    
    static fromDto(source: string, logLevel: Sparrow.Logging.LogMode) {
        const item = new loggerCategory();
        item.source(source);
        item.logLevel(logLevel);
        return item;
    }
}

class configureLoggersDialog extends dialogViewModelBase {
    view = require("views/manage/configureLoggersDialog.html");
    
    private readonly loggers: string[];
    private readonly configuration = ko.observableArray<loggerCategory>([]);
    
    newSourceName = ko.observable<string>();
    
    constructor(availableLoggers: adminLogsLoggersResponse, configuration: adminLogsLoggersConfigurationResponse) {
        super();
        
        this.loggers = this.extractLoggerNames(availableLoggers.Loggers ?? {});
        
        this.configuration(Object.entries<Sparrow.Logging.LogMode>(configuration.Loggers ?? {}).map(kv => loggerCategory.fromDto(kv[0], kv[1])));
        
        _.bindAll(this, "deleteItem", "addLogSourceFromDropdown");
        
        this.newSourceName.extend({
            required: true
        });
    }

    addLogSource() {
        const newItem = loggerCategory.fromDto(this.newSourceName(), "Operations");
        this.configuration.push(newItem);
        this.newSourceName("");
        this.newSourceName.clearError();
    }

    addLogSourceFromDropdown(source: string): void {
        this.newSourceName(source);
        this.addLogSource();
    }

    createLogSourceAutoCompleter() {
        return ko.pureComputed(() => {
            const key = this.newSourceName();
            
            const maxAutocompleteItems = 50;
            
            const existingSources = this.configuration().map(x => x.source());
            
            const completions = this.loggers.filter(x => !existingSources.includes(x));
            
            if (key) {
                return completions.filter(x => x.startsWith(key)).slice(0, maxAutocompleteItems);
            } else {
                return completions.slice(0, maxAutocompleteItems);
            }
        });
    }
    
    private extractLoggerNames(loggers: dictionary<adminLogsLoggerDto>) {
        const result: string[] = [];

        const queue: Array<adminLogsLoggerDto> = [...Object.values(loggers)];
        while (queue.length) {
            const item = queue.pop();
            result.push(item.Source ? item.Source + "." + item.Name : item.Name);

            if (item.Loggers) {
                queue.push(...Object.values(item.Loggers));    
            }
        }
        
        return result;
    }

    attached() {
        // empty by design, so that pressing enter does not call save button click
    }

    close() {
        dialog.close(this);
    }

    deleteItem(item: loggerCategory) {
        this.configuration.remove(item);
    }
    
    save() {
        const result: dictionary<Sparrow.Logging.LogMode> = { 
        };
        
        this.configuration().forEach(item => {
            result[item.source()] = item.logLevel();
        });
        
        dialog.close(this, result);
    }
}

export = configureLoggersDialog;
