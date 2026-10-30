import { useEffect } from "react";
import { act, rtlRender } from "test/rtlTestUtils";
import NotificationCenterModalHost from "./NotificationCenterModalHost";

interface GreetingProps {
    name: string;
    onMount?: () => void;
}

function Greeting({ name, onMount }: GreetingProps) {
    useEffect(() => {
        onMount?.();
    }, [onMount]);

    return <span>Hello {name}</span>;
}

function greetingView(props: GreetingProps): ReactInKnockout<typeof Greeting> {
    return ko.pureComputed(() => ({ component: Greeting, props }));
}

describe("NotificationCenterModalHost", () => {
    it("renders the current view and removes it when the view is cleared", () => {
        const modalView = ko.observable<ReactInKnockout<any>>(null);
        const { screen } = rtlRender(<NotificationCenterModalHost modalView={modalView} />);

        act(() => modalView(greetingView({ name: "Ann" })));
        expect(screen.getByText("Hello Ann")).toBeInTheDocument();

        act(() => modalView(null));
        expect(screen.queryByText(/Hello/)).not.toBeInTheDocument();
    });

    it("updates the shown view when its props change", () => {
        const name = ko.observable("Ann");
        const modalView = ko.observable<ReactInKnockout<any>>(
            ko.pureComputed(() => ({ component: Greeting, props: { name: name() } }))
        );
        const { screen } = rtlRender(<NotificationCenterModalHost modalView={modalView} />);

        act(() => name("Bob"));

        expect(screen.getByText("Hello Bob")).toBeInTheDocument();
    });

    it("lets the shown view close itself from an effect", () => {
        const modalView = ko.observable<ReactInKnockout<any>>(null);
        const { screen } = rtlRender(<NotificationCenterModalHost modalView={modalView} />);

        act(() => modalView(greetingView({ name: "Ann", onMount: () => modalView(null) })));

        expect(screen.queryByText(/Hello/)).not.toBeInTheDocument();
    });
});
