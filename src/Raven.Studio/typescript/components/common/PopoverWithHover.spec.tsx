import { useState } from "react";
import Popover from "react-bootstrap/Popover";
import { act, fireEvent, rtlRender, waitFor } from "test/rtlTestUtils";
import { PopoverWithHover } from "./PopoverWithHover";

const VIEWPORT = { width: 1200, height: 800 };
const POPOVER_SIZE = { width: 300, height: 200 };

function ValueCell() {
    const [target, setTarget] = useState<HTMLElement>();

    return (
        <>
            <div ref={setTarget}>10 - 4 oz boxes</div>
            <PopoverWithHover target={target} placement="bottom-start">
                <Popover.Body>&quot;10 - 4 oz boxes&quot;</Popover.Body>
            </PopoverWithHover>
        </>
    );
}

function PreparedValueCell({ prepare, openDelayInMs }: { prepare: () => Promise<unknown>; openDelayInMs?: number }) {
    const [target, setTarget] = useState<HTMLElement>();

    return (
        <>
            <div ref={setTarget}>[...]5</div>
            <PopoverWithHover target={target} placement="bottom-start" prepare={prepare} openDelayInMs={openDelayInMs}>
                <Popover.Body>Five order lines</Popover.Body>
            </PopoverWithHover>
        </>
    );
}

function DocumentIdCell({ isLoaded }: { isLoaded: boolean }) {
    const [target, setTarget] = useState<HTMLElement>();

    return (
        <>
            <div ref={setTarget}>orders/1-A</div>
            <PopoverWithHover target={target} placement="right">
                <Popover.Body>{isLoaded ? "Company: companies/65-A" : "Loading"}</Popover.Body>
            </PopoverWithHover>
        </>
    );
}

describe("PopoverWithHover with content to prepare", () => {
    const holdPreparation = () => {
        let finish: () => void;
        const prepare = jest.fn(() => new Promise<void>((resolve) => (finish = resolve)));
        return { prepare, finishPreparing: () => act(async () => finish()) };
    };

    const waitPastHoverDelay = () =>
        act(async () => {
            jest.advanceTimersByTime(600);
        });

    beforeEach(() => {
        jest.useFakeTimers();
    });

    const advanceBy = (ms: number) =>
        act(async () => {
            jest.advanceTimersByTime(ms);
        });

    it("opens after its open delay while its content is still being prepared", async () => {
        const { prepare } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} openDelayInMs={700} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await advanceBy(400);
        expect(prepare).toHaveBeenCalled();
        expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

        await advanceBy(300);

        expect(screen.getByRole("tooltip")).toHaveTextContent("Five order lines");
    });

    it("waits for its open delay when its content is ready sooner", async () => {
        const { prepare, finishPreparing } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} openDelayInMs={700} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await advanceBy(400);
        await finishPreparing();
        expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

        await advanceBy(300);

        expect(screen.getByRole("tooltip")).toBeInTheDocument();
    });

    it("stays closed when the pointer leaves before its open delay", async () => {
        const { prepare } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} openDelayInMs={700} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await advanceBy(400);
        fireEvent.mouseLeave(screen.getByText("[...]5"));
        await advanceBy(1000);

        expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it("opens once its content is ready", async () => {
        const { prepare, finishPreparing } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await waitPastHoverDelay();

        expect(prepare).toHaveBeenCalled();
        expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

        await finishPreparing();

        expect(await screen.findByRole("tooltip")).toHaveTextContent("Five order lines");
    });

    it("opens when its content can't be prepared", async () => {
        const prepare = jest.fn(() => Promise.reject(new Error("Unable to load the order lines")));
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await waitPastHoverDelay();

        expect(await screen.findByRole("tooltip")).toHaveTextContent("Five order lines");
    });

    it("waits for a new rest when the pointer comes back while its content is still being prepared", async () => {
        const { prepare, finishPreparing } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} />);
        const cell = screen.getByText("[...]5");

        fireEvent.mouseEnter(cell);
        await advanceBy(400);
        fireEvent.mouseLeave(cell);
        fireEvent.mouseEnter(cell);
        await advanceBy(100);
        await finishPreparing();
        expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

        await advanceBy(300);
        await finishPreparing();

        expect(prepare).toHaveBeenCalledTimes(2);
        expect(screen.getByRole("tooltip")).toHaveTextContent("Five order lines");
    });

    it("keeps listening to the pointer when it gets a new prepare function", () => {
        const { screen, rerender } = rtlRender(<PreparedValueCell prepare={() => Promise.resolve()} />);
        const addEventListener = jest.spyOn(screen.getByText("[...]5"), "addEventListener");

        rerender(<PreparedValueCell prepare={() => Promise.resolve()} />);

        expect(addEventListener).not.toHaveBeenCalled();
    });

    it("prepares its content with the latest prepare function", async () => {
        const firstPrepare = jest.fn(() => Promise.resolve());
        const latestPrepare = jest.fn(() => Promise.resolve());
        const { screen, rerender } = rtlRender(<PreparedValueCell prepare={firstPrepare} />);

        rerender(<PreparedValueCell prepare={latestPrepare} />);
        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await waitPastHoverDelay();

        expect(latestPrepare).toHaveBeenCalledTimes(1);
        expect(firstPrepare).not.toHaveBeenCalled();
    });

    it("does not prepare its content for a pointer passing over it", async () => {
        const { prepare } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await act(async () => {
            jest.advanceTimersByTime(100);
        });
        fireEvent.mouseLeave(screen.getByText("[...]5"));
        await waitPastHoverDelay();

        expect(prepare).not.toHaveBeenCalled();
    });

    it("does not prepare its content once its target is gone", async () => {
        const { prepare } = holdPreparation();
        const { screen, unmount } = rtlRender(<PreparedValueCell prepare={prepare} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        unmount();
        await waitPastHoverDelay();

        expect(prepare).not.toHaveBeenCalled();
    });

    it("stays closed when the pointer leaves before its content is ready", async () => {
        const { prepare, finishPreparing } = holdPreparation();
        const { screen } = rtlRender(<PreparedValueCell prepare={prepare} />);

        fireEvent.mouseEnter(screen.getByText("[...]5"));
        await waitPastHoverDelay();
        fireEvent.mouseLeave(screen.getByText("[...]5"));
        await finishPreparing();
        await waitPastHoverDelay();

        expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    });
});

const ELEMENT_SIZE_PROPERTIES = ["offsetWidth", "offsetHeight"];

describe("PopoverWithHover", () => {
    const originalElementSizes = ELEMENT_SIZE_PROPERTIES.map((property) =>
        Object.getOwnPropertyDescriptor(HTMLElement.prototype, property)
    );

    beforeEach(() => {
        jest.spyOn(document.documentElement, "clientWidth", "get").mockReturnValue(VIEWPORT.width);
        jest.spyOn(document.documentElement, "clientHeight", "get").mockReturnValue(VIEWPORT.height);
        Object.defineProperty(HTMLElement.prototype, "offsetWidth", { configurable: true, value: POPOVER_SIZE.width });
        Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
            configurable: true,
            value: POPOVER_SIZE.height,
        });
    });

    afterEach(() => {
        ELEMENT_SIZE_PROPERTIES.forEach((property, index) =>
            Object.defineProperty(HTMLElement.prototype, property, originalElementSizes[index])
        );
        jest.restoreAllMocks();
    });

    it("opens above its target when there is no room below it", async () => {
        const { screen, user } = rtlRender(<ValueCell />);
        const cell = screen.getByText("10 - 4 oz boxes");
        jest.spyOn(cell, "getBoundingClientRect").mockReturnValue(
            DOMRect.fromRect({ x: 100, y: VIEWPORT.height - 40, width: 120, height: 20 })
        );

        await user.hover(cell);
        const popover = await screen.findByRole("tooltip");

        await waitFor(() => expect(popover).toHaveClass("bs-popover-top"));
    });

    it("opens below its target when there is room", async () => {
        const { screen, user } = rtlRender(<ValueCell />);
        const cell = screen.getByText("10 - 4 oz boxes");
        jest.spyOn(cell, "getBoundingClientRect").mockReturnValue(
            DOMRect.fromRect({ x: 100, y: 100, width: 120, height: 20 })
        );

        await user.hover(cell);
        const popover = await screen.findByRole("tooltip");

        await waitFor(() => expect(popover).toHaveClass("bs-popover-bottom"));
    });

    it("stays centered on its target when its content changes size", async () => {
        let popoverHeight = POPOVER_SIZE.height;
        Object.defineProperty(HTMLElement.prototype, "offsetHeight", { configurable: true, get: () => popoverHeight });
        const { screen, user, rerender } = rtlRender(<DocumentIdCell isLoaded={false} />);
        const cell = screen.getByText("orders/1-A");
        jest.spyOn(cell, "getBoundingClientRect").mockReturnValue(
            DOMRect.fromRect({ x: 100, y: 400, width: 120, height: 20 })
        );

        await user.hover(cell);
        const popover = await screen.findByRole("tooltip");
        await waitFor(() => expect(popover.style.top).toBe("310px"));

        popoverHeight = 100;
        rerender(<DocumentIdCell isLoaded />);

        await waitFor(() => expect(popover.style.top).toBe("360px"));
    });
});
