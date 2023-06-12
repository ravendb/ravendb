export default class MockDirtyFlagHook {
    private _isDirty: boolean;

    private readonly _mock = (isDirty: boolean) => {
        this._isDirty = isDirty;
    };

    constructor() {
        this._isDirty = false;
    }

    get isDirty(): boolean {
        return this._isDirty;
    }

    get mock() {
        return this._mock;
    }
}
