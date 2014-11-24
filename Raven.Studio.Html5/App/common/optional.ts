class optional<T> {
    constructor(private instance: T) {

    }

    static val<T>(value: T): optional<T> {
        return new optional(value);
    }

    bind<TReturn>(fetcher: (i: T) => TReturn): optional<TReturn> {
        if (this.instance !== null && this.instance !== undefined) {
            var val = fetcher(this.instance);
            return new optional(val);
        }

        return new optional(null);
    }

    toString(): string {
        if (this.instance !== null && this.instance !== undefined) {
            return this.instance.toString();
        }

        return "";
    }
}

export = optional;