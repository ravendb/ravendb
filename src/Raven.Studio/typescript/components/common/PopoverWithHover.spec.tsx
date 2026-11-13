import { useState } from "react";
import Popover from "react-bootstrap/Popover";
import { act, fireEvent, rtlRender, waitFor } from "test/rtlTestUtils";
import { PopoverWithHover } from "./PopoverWithHover";

const viewport = { width: 1200, height: 800 };
const popoverSize = { width: 300, height: 200 };

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

function PreparedValueCell({ prepare }: { prepare: () => Promise<unknown> }) {
    const [target, setTarget] = useState<HTMLElement>();

    return (
        <>
            <div ref={setTarget}>[...]5</div>
            <PopoverWithHover target={target} placement="bottom-start" prepare={prepare}>
                <Popover.Body>Five order lines</Popover.Body>
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
            jest.advanceTimersByTime(500);
        });

    beforeEach(() => {
        jest.useFakeTimers();
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

const elementSizeProperties = ["offsetWidth", "offsetHeight"] as const;

describe("PopoverWithHover", () => {
    const originalElementSizes = elementSizeProperties.map((property) =>
        Object.getOwnPropertyDescriptor(HTMLElement.prototype, property)
    );

    beforeEach(() => {
        jest.spyOn(document.documentElement, "clientWidth", "get").mockReturnValue(viewport.width);
        jest.spyOn(document.documentElement, "clientHeight", "get").mockReturnValue(viewport.height);
        Object.defineProperty(HTMLElement.prototype, "offsetWidth", { configurable: true, value: popoverSize.width });
        Object.defineProperty(HTMLElement.prototype, "offsetHeight", { configurable: true, value: popoverSize.height });
    });

    afterEach(() => {
        elementSizeProperties.forEach((property, index) =>
            Object.defineProperty(HTMLElement.prototype, property, originalElementSizes[index])
        );
        jest.restoreAllMocks();
    });

    it("opens above its target when there is no room below it", async () => {
        const { screen, user } = rtlRender(<ValueCell />);
        const cell = screen.getByText("10 - 4 oz boxes");
        jest.spyOn(cell, "getBoundingClientRect").mockReturnValue(
            DOMRect.fromRect({ x: 100, y: viewport.height - 40, width: 120, height: 20 })
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
});
