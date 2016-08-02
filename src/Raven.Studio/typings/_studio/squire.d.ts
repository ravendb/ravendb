
interface Squire {
    new (): Squire;
    new(context: string): Squire;
    mock(name: string, mock: any): Squire;
    mock(mocks: { [name: string]: any }): Squire;
    require(dependencies: string[], callback: Function, errback?: Function): Squire;
    store(name: string | string[]): Squire;
    clean(): Squire;
    clean(name: string | string[]): Squire;
    remove(): String;
    run(dependencies: string[], test: Function): (done: Function) => void;

    Helpers: {
        returns<T>(what: T): () => T;
        constructs<T>(what: T): () => (() => T);
    }
}

declare module "Squire" {
    var injector: Squire;

    export = injector;


}

declare var injector: Squire;


