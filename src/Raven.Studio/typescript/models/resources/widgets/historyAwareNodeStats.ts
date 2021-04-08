/// <reference path="../../../../typings/tsd.d.ts"/>

class historyAwareNodeStats<T extends Raven.Server.Dashboard.Cluster.AbstractClusterDashboardNotification> {
    readonly tag: string;

    connectedAt = ko.observable<Date>();
    currentItem = ko.observable<cachedDateValue<T>>();

    mouseOver = ko.observable<boolean>(false);
    history: cachedDateValue<T>[] = [];
    connectionHistory: [Date, Date][] = [];
    
    spinner = ko.pureComputed(() => {
        const noData = !this.currentItem();
        const mouseOver = this.mouseOver();
        return !mouseOver && noData;
    })

    constructor(tag: string) {
        this.tag = tag;
    }

    onConnectionStatusChanged(connected: boolean) {
        if (connected) {
            this.connectedAt(new Date());
        } else {
            const connectionDate = this.connectedAt();
            this.connectedAt(null);
            if (!this.mouseOver()) {
                this.currentItem(null);
            }
            this.connectionHistory.push([connectionDate, new Date()]);
        }
    }

    onData(data: T) {
        const date = moment.utc(data.Date).toDate();
        const newItem = {
            date,
            value: data
        };
        this.history.push(newItem);

        if (!this.mouseOver() && this.connectedAt()) {
            this.currentItem(newItem);
        }

        this.maybeTrimHistory();
    }

    // null means show latest one
    showItemAtDate(date: Date|null) {
        this.mouseOver(!!date);
        if (date) {
            if (!this.wasConnected(date)) {
                this.currentItem(null);
                return;
            }
            const time = date.getTime();
            if (history.length) {
                for (let i = this.history.length - 1; i >= 0; i--) {
                    const item = this.history[i];
                    if (item.date.getTime() < time) {
                        // found index to use
                        this.currentItem(item);
                        return;
                    }
                }
            }

            this.currentItem(null);
        } else {
            // use latest data 
            if (history.length && this.connectedAt()) {
                this.currentItem(this.history[this.history.length - 1]);
            } else {
                this.currentItem(null);
            }
        }
    }
    
    // don't be so strict here - due to potential time skews between local machine and servers
    private wasConnected(date: Date): boolean {
        const time = date.getTime();
        const currentConnection = this.connectedAt();
        const maxSkew = 3000;

        if (currentConnection && currentConnection.getTime() - maxSkew < time) {
            return true;
        }

        for (const [start, end] of this.connectionHistory) {
            const startWithSkew = start.getTime() - maxSkew;
            const endWithSkew = end.getTime() + maxSkew;
            if (startWithSkew <= time && time <= endWithSkew) {
                return true;
            }
        }

        return false;
    }

    private maybeTrimHistory() {
        if (this.history.length > 5000) {
            this.history.slice(3000);
        }

        if (this.connectionHistory.length > 1000) {
            this.connectionHistory.slice(800);
        }
    }

    protected noDataText(): string|null {
        const currentItem = this.currentItem();
        const mouseOver = this.mouseOver();
        if (currentItem) {
            return null;
        } else {
            return mouseOver ? "No data" : "Connecting...";
        }
    }
    
    protected conditionalDataExtractor<S>(accessor: (value: T) => S, opts: { customNoData?: string } = {}) {
        return ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return opts.customNoData || noData;
            }
            return accessor(this.currentItem().value);
        });
    }

    protected dataExtractor<S>(accessor: (value: T) => S) {
        return ko.pureComputed(() => {
            const item = this.currentItem();
            return item ? accessor(item.value) : null;
        });
    }
}

export = historyAwareNodeStats;
