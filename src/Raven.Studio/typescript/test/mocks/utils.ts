import { MockedValue } from "test/mocks/services/AutoMockService";

export function createValue<T>(value: MockedValue<T>, defaultValue: T): T {
    if (value instanceof Function) {
        const v = defaultValue;
        value(v);
        return v;
    } else {
        return value ?? defaultValue;
    }
}
