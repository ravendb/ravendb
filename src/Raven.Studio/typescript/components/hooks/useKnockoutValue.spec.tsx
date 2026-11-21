import { act, rtlRender } from "test/rtlTestUtils";
import { useKnockoutValue } from "./useKnockoutValue";

function Value({ subscribable }: { subscribable: KnockoutObservable<string> }) {
    return <span>{useKnockoutValue(subscribable)}</span>;
}

function Count({ subscribable }: { subscribable: KnockoutObservableArray<string> }) {
    return <span>{useKnockoutValue(subscribable).length} items</span>;
}

describe("useKnockoutValue", () => {
    it("re-renders when the observable changes", () => {
        const observable = ko.observable("first");
        const { screen } = rtlRender(<Value subscribable={observable} />);

        act(() => observable("second"));

        expect(screen.getByText("second")).toBeInTheDocument();
    });

    it("shows a change made between the render and the subscription", () => {
        const observable = ko.observable("first");
        const subscribe = observable.subscribe.bind(observable);
        jest.spyOn(observable, "subscribe").mockImplementation((callback: (value: string) => void) => {
            observable("second");
            return subscribe(callback);
        });

        const { screen } = rtlRender(<Value subscribable={observable} />);

        expect(screen.getByText("second")).toBeInTheDocument();
    });

    it("does not render again after mounting when nothing changes", () => {
        const observable = ko.observable("first");
        const onRender = jest.fn();

        function RenderCounter() {
            onRender();
            return <span>{useKnockoutValue(observable)}</span>;
        }

        rtlRender(<RenderCounter />);

        expect(onRender).toHaveBeenCalledOnce();
    });

    it("re-renders when an observable array is changed in place", () => {
        const observableArray = ko.observableArray(["first"]);
        const { screen } = rtlRender(<Count subscribable={observableArray} />);

        act(() => {
            observableArray.push("second");
        });

        expect(screen.getByText("2 items")).toBeInTheDocument();
    });
});
