/// <reference path="../../../typings/tsd.d.ts"/>

interface listViewController<T> { 

    pushElements(item: T[]): void;
    reset(): void;
    getItems: () => Map<number, T>;
    scrollDown: () => void;
    getTotalCount: () => number;
}

export = listViewController;
