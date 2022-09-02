/// <reference path="../../typings/tsd.d.ts"/>

class chunkFetcher<T> {

    constructor(private fetcher: (skip: number, take: number) => JQueryPromise<T[]>) {
        if (!fetcher) {
            throw new Error("Fetcher must be specified.");
        }
    }

    task = $.Deferred<T[]>();
    result: T[] = [];


    execute(): JQueryPromise<T[]> {
        const skip = 0;
        const take = 1024;
        this.fetchChunk(skip, take);
        return this.task;
    }

    fetchChunk(skip: number, take: number) {
        this.fetcher(skip, take)
            .fail(x => this.task.reject(x))
            .done(data => {
                this.result.push(...data);
                if (data.length === take) {
                    this.fetchChunk(skip + take, take);
                } else {
                    this.task.resolve(this.result);
                }
            });
    }
}

export = chunkFetcher;
