/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");

class lastUsedAutocomplete {

    private readonly contextName: string;
    private readonly backingField: KnockoutObservable<string>;
    private readonly maxLength: number;

    constructor(contextName: string, backingField: KnockoutObservable<string>, maxLength: number = 5) {
        this.contextName = contextName;
        this.backingField = backingField;
        this.maxLength = maxLength;
        
        _.bindAll(this, "complete");
    }
    
    recordUsage() {
        const key = this.backingField();
        if (!key) {
            return;
        }
        
        const existingItems = this.getSavedItems();
        const keyLower = key.toLocaleLowerCase();
        const exitingItem = existingItems.find(x => x.toLocaleLowerCase() === keyLower);
        
        if (exitingItem) {
            _.pull(existingItems, exitingItem);
        }

        // append at the beginning of the list
        existingItems.unshift(key);
        
        if (existingItems.length > this.maxLength) {
            existingItems.length = this.maxLength;
        }
        
        this.saveItems(existingItems);
    }
    
    complete(value: string) {
        this.backingField(value);
    }
    
    createCompleter(): KnockoutComputed<string[]> {
        return ko.pureComputed(() => {
            const savedItems = this.getSavedItems();
            
            const filter = this.backingField();
            
            if (filter) {
                const filterLower = filter.toLocaleLowerCase();
                return savedItems.filter(x => x.toLocaleLowerCase().includes(filterLower));
            } else {
                return savedItems;
            }
        });
    }
    
    private getLocalStorageKey() {
        return storageKeyProvider.storageKeyFor("last-used-autocomplete-" + this.contextName);
    }
    
    private getSavedItems(): string[] {
        const localStorageKeyName = this.getLocalStorageKey();
        
        let savedValue = localStorage.getObject(localStorageKeyName);
        if (!savedValue || savedValue instanceof Array === false) {
            savedValue = [];
            this.saveItems([]);
        }
        
        return savedValue;
    }
    
    private saveItems(items: string[]) {
        const localStorageKeyName = this.getLocalStorageKey();
        
        localStorage.setObject(localStorageKeyName, items);
    }
}

export = lastUsedAutocomplete;
