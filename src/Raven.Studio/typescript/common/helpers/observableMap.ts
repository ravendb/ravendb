type mapItem<K, V> = {
    key: K;
    value: V;
}

class observableMap<K, V> {

    private store = ko.observableArray<mapItem<K, V>>([]);

    set(key: K, value: V) {
        const idx = this.store().findIndex(x => x.key === key);
        if (idx !== -1) {
            this.store.splice(idx, 0, { key: key, value: value });
        } else {
            this.store.push({ key: key, value: value });
        }
    }

    get(key: K) {
        return this.store().find(x => x.key === key);
    }

    delete(key: K) {
        const item = this.get(key);
        if (item) {
            this.store.remove(item);
        }
    }

    watchFor(key: K) {
        return ko.pureComputed(() => {
            const item = this.get(key);
            return item ? item.value : null;
        });
    }

    clear() {
        this.store.removeAll();
    }
}

export = observableMap;