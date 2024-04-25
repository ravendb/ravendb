import { timeAwareEWMA } from "./timeAwareEWMA";

describe("timeAwareEWMA", () => {
    beforeAll(() => {
        jest.useFakeTimers();
    });

    afterAll(() => {
        jest.useRealTimers();
    });

    it("can calculate ewma when server doesn't responds for more than 4 seconds", () => {
        const ewma = new timeAwareEWMA(2_000);
        
        ewma.handleServerTick(10_000);

        // after 3 seconds no change
        jest.advanceTimersByTime(3_000);
        expect(ewma.value()).toBe(10_000);

        // after 4 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(2_500);

        // after 5 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(1_767);

        // after 6 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(1_249);

        // after 7 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(883);

        // after 8 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(624);

        // after 9 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(441);

        // after 10 seconds
        jest.advanceTimersByTime(1_000);
        expect(ewma.value()).toBe(0);
    });

    it("can calculate value when server responds every second", () => {
        const ewma = new timeAwareEWMA(2_000);

        ewma.handleServerTick(10_000);

        // after 1 second
        jest.advanceTimersByTime(1_000);
        ewma.handleServerTick(15_000);
        expect(ewma.value()).toBe(15_000);

        // after 2 seconds
        jest.advanceTimersByTime(1_000);
        ewma.handleServerTick(5_000);
        expect(ewma.value()).toBe(5_000);
    });

    it("can calculate value when server responds at irregular timestamps", () => {
        const ewma = new timeAwareEWMA(2_000);

        ewma.handleServerTick(10_000);

        // after 0.5 second (value / 0.5)
        jest.advanceTimersByTime(500);
        ewma.handleServerTick(10_000);
        expect(ewma.value()).toBe(20_000);

        // after 3 seconds (value / 3)
        jest.advanceTimersByTime(3_000);
        ewma.handleServerTick(9_000);
        expect(ewma.value()).toBe(3_000);
    });

    it("can calculate value right away", () => {
        const ewma = new timeAwareEWMA(2_000);

        ewma.handleServerTick(10_000);
        expect(ewma.value()).toBe(10_000);
    });
});

