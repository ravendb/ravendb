/// <reference path="../../../../typings/tsd.d.ts"/>

import moment = require("moment");

type matchMode = "closestPrevious" | "exact";

class historyAwareNodeStats<T extends { Date: string }> {
    readonly tag: string;
    readonly matchMode: matchMode;

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

    constructor(tag: string, matchMode: matchMode = "closestPrevious") {
        this.tag = tag;
        this.matchMode = matchMode;
    }

    onConnectionStatusChanged(connected: true, serverTime: Date): void;
    onConnectionStatusChanged(connected: false): void;
    onConnectionStatusChanged(connected: boolean, serverTime?: Date): void {
        if (connected) {
            this.connectedAt(serverTime);
        } else {
            const connectionDate = this.connectedAt();
            this.connectedAt(null);
            if (!this.mouseOver()) {
                this.currentItem(null);
            }
            if (this.history.length) {
                const lastHistoryItem = this.history[this.history.length - 1].date;
                if (connectionDate.getTime() <= lastHistoryItem.getTime()) {
                    this.connectionHistory.push([connectionDate, lastHistoryItem]);
                }
            }
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
    showItemAtDate(date: ClusterWidgetAlignedDate|null, fallbackToCurrent = true) {
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
                    switch (this.matchMode) {
                        case "closestPrevious": {
                            if (item.date.getTime() <= time) {
                                // found index to use
                                this.currentItem(item);
                                return;
                            }
                            break;
                        }
                        case "exact": {
                            if (item.date.getTime() === time) {
                                // found index to use
                                this.currentItem(item);
                                return;
                            }
                            break;
                        }
                        default: 
                            throw new Error("Unhandled match mode:" + this.matchMode);
                    }
                    
                }
            }

            this.currentItem(null);
        } else {
            // use latest data
            if (history.length && this.connectedAt() && fallbackToCurrent) {
                this.currentItem(this.history[this.history.length - 1]);
            } else {
                this.currentItem(null);
            }
        }
    }
    
    private wasConnected(date: Date): boolean {
        const time = date.getTime();
        const currentConnection = this.connectedAt();

        if (currentConnection && currentConnection.getTime() < time) {
            return true;
        }

        for (const [start, end] of this.connectionHistory) {
            if (start.getTime() <= time && time <= end.getTime()) {
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
