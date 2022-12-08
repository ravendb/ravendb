import MockInstance = jest.MockInstance;

type ServiceMocks<T extends object> = {
    [K in keyof T]: T[K] extends (...args: any) => any ? jest.MockInstance<ReturnType<T[K]>, Parameters<T[K]>> : never;
};

/**
 * This interface allows to pass value explicitly or customize default value using builder
 */
export type MockedValue<T> = ((defaultValue: T) => void) | T;

export abstract class AutoMockService<T extends object> {
    protected mocks = {} as ServiceMocks<T>;

    protected constructor(serviceToMock: T) {
        const methods = AutoMockService.getMethods(serviceToMock);
        methods.forEach((method) => {
            (this.mocks as any)[method as any] = jest.fn();
        });
    }

    protected createValue<T>(value: MockedValue<T>, defaultValue: T): T {
        if (value instanceof Function) {
            const v = defaultValue;
            value(v);
            return v;
        } else {
            return value ?? defaultValue;
        }
    }

    protected mockResolvedValue<T>(mock: MockInstance<T | Promise<T>, any>, value: MockedValue<T>, defaultValue: T) {
        const dto = this.createValue(value, defaultValue);
        mock.mockResolvedValue(dto);
        return dto;
    }

    private static getMethods(obj: object) {
        //TODO: only services methods! + check if it is service!
        const properties = new Set<string>();
        let currentObj = obj;
        do {
            Object.getOwnPropertyNames(currentObj).map((item) => properties.add(item));
            currentObj = Object.getPrototypeOf(currentObj);
        } while (currentObj);
        return [...properties.keys()].filter((item) => typeof (obj as any)[item as any] === "function");
    }

    get mock() {
        return this.mocks as T;
    }
}
