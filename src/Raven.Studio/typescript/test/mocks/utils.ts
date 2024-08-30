import { MockedValue, ServiceMocks } from "test/mocks/services/AutoMockService";
import { mockServices } from "test/mocks/services/MockServices";

import { MockedObject } from "@storybook/test";

export function createValue<T>(value: MockedValue<T>, defaultValue: T): T {
    if (value instanceof Function) {
        const v = defaultValue;
        value(v);
        return v;
    } else {
        return value ?? defaultValue;
    }
}

type DebugItem = {
    serviceName: string;
    methodName: string;
    hasImplementation: boolean;
    callsCount: number;
};

/**
 * Displays info about mocks which being called, but implementation wasn't provided
 */
export function debugMocks() {
    const context = mockServices.context;
    const services = Object.keys(context);

    const results: DebugItem[] = [];

    services.forEach((serviceName) => {
        const mockService = (context as any)[serviceName] as ServiceMocks<any>;
        const mockMethods = Object.keys(mockService);
        mockMethods.forEach((methodName) => {
            const mockMethod = mockService[methodName] as MockedObject<any>;
            const callsCount = mockMethod.mock.calls.length;
            const hasImplementation = !!mockMethod.getMockImplementation();

            results.push({
                serviceName,
                methodName,
                callsCount,
                hasImplementation,
            });
        });
    });

    const info = results
        .filter((x) => x.callsCount > 0 && !x.hasImplementation)
        .map((x) => x.serviceName + "::" + x.methodName + ": calls = " + x.callsCount);

    if (info.length > 0) {
        console.warn("Following mocks were called but not implemented: \r\n" + info.join("\r\n"));
    }
}
