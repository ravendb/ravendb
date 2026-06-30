import React from "react";
import { rtlRender, act } from "test/rtlTestUtils";
import { useScrollSpy } from "./useScrollSpy";

let lastObserver: MockIO | null = null;

function captureObserver(observer: MockIO) {
    lastObserver = observer;
}

class MockIO {
    callback: IntersectionObserverCallback;
    elements: Element[] = [];
    constructor(cb: IntersectionObserverCallback) {
        this.callback = cb;
        captureObserver(this);
    }
    observe(el: Element) {
        this.elements.push(el);
    }
    unobserve(el: Element) {
        this.elements = this.elements.filter((e) => e !== el);
    }
    disconnect() {
        this.elements = [];
    }
    fire(entries: Array<{ id: string; isIntersecting: boolean }>) {
        this.callback(
            entries.map((e) => ({ target: document.getElementById(e.id)!, isIntersecting: e.isIntersecting })) as any,
            this as any
        );
    }
}

function Harness({ ids }: { ids: string[] }) {
    const active = useScrollSpy(ids);
    return (
        <div>
            {ids.map((id) => (
                <div key={id} id={id}>
                    {id}
                </div>
            ))}
            <span data-testid="active">{active ?? "none"}</span>
        </div>
    );
}

describe("useScrollSpy", () => {
    beforeEach(() => {
        lastObserver = null;
        (window as any).IntersectionObserver = MockIO;
    });

    it("defaults the active id to the first id", () => {
        const { screen } = rtlRender(<Harness ids={["a", "b", "c"]} />);
        expect(screen.getByTestId("active")).toHaveTextContent("a");
    });

    it("activates the first intersecting id in document order", () => {
        const { screen } = rtlRender(<Harness ids={["a", "b", "c"]} />);
        act(() => lastObserver!.fire([{ id: "b", isIntersecting: true }]));
        expect(screen.getByTestId("active")).toHaveTextContent("b");
    });

    it("falls back to the next visible id when one leaves the viewport", () => {
        const { screen } = rtlRender(<Harness ids={["a", "b", "c"]} />);
        act(() =>
            lastObserver!.fire([
                { id: "b", isIntersecting: true },
                { id: "c", isIntersecting: true },
            ])
        );
        expect(screen.getByTestId("active")).toHaveTextContent("b");
        act(() => lastObserver!.fire([{ id: "b", isIntersecting: false }]));
        expect(screen.getByTestId("active")).toHaveTextContent("c");
    });

    it("activates the last id when the scroll container reaches the bottom", () => {
        let scrollTop = 0;
        const root = document.createElement("div");
        Object.defineProperty(root, "scrollTop", { get: () => scrollTop, configurable: true });
        Object.defineProperty(root, "scrollHeight", { get: () => 1000, configurable: true });
        Object.defineProperty(root, "clientHeight", { get: () => 300, configurable: true });
        document.body.appendChild(root);

        function HarnessWithRoot({ ids }: { ids: string[] }): React.ReactElement {
            const active = useScrollSpy(ids, { root });
            return <span data-testid="active">{active ?? "none"}</span>;
        }

        const { screen } = rtlRender(<HarnessWithRoot ids={["a", "b", "c"]} />);
        expect(screen.getByTestId("active")).toHaveTextContent("a");

        scrollTop = 700; // 1000 - 700 - 300 = 0 -> bottom reached
        act(() => root.dispatchEvent(new Event("scroll")));
        expect(screen.getByTestId("active")).toHaveTextContent("c");

        document.body.removeChild(root);
    });
});
