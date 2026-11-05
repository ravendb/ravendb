import { NotificationDetailsActions } from "./notificationDetailsProvider";

export function mockDetailsActions(): NotificationDetailsActions {
    return {
        close: jest.fn(),
        dismiss: jest.fn(),
        postpone: jest.fn(() => Promise.resolve()),
    };
}

export function mockTableWidth(widthInPx = 1000) {
    let restore: jest.SpyInstance;

    beforeEach(() => {
        restore = jest
            .spyOn(HTMLElement.prototype, "getBoundingClientRect")
            .mockReturnValue({ width: widthInPx, height: 500 } as DOMRect);
    });

    afterEach(() => restore.mockRestore());
}
