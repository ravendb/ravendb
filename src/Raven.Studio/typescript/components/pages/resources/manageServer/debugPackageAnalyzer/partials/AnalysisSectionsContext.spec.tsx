import React, { useEffect } from "react";
import { rtlRender, act } from "test/rtlTestUtils";
import { AnalysisSectionsProvider, useAnalysisSections } from "./AnalysisSectionsContext";

// Registers a raw element that already sits in the DOM, in a chosen order.
function Register({
    id,
    label,
    getEl,
}: {
    id: string;
    label: string;
    getEl: () => HTMLElement;
}): React.ReactElement | null {
    const { register, unregister } = useAnalysisSections();
    useEffect(() => {
        register({ id, label, element: getEl() });
        return () => unregister(id);
    }, [id, label, register, unregister, getEl]);
    return null;
}

function EntryList(): React.ReactElement {
    const { entries } = useAnalysisSections();
    return <span data-testid="order">{entries.map((e) => e.id).join(",")}</span>;
}

describe("AnalysisSectionsContext", () => {
    it("returns entries in DOM order even when registered out of order", () => {
        const host = document.createElement("div");
        const first = document.createElement("div");
        const second = document.createElement("div");
        host.appendChild(first);
        host.appendChild(second);
        document.body.appendChild(host);

        const { screen } = rtlRender(
            <AnalysisSectionsProvider>
                <Register id="second" label="Second" getEl={() => second} />
                <Register id="first" label="First" getEl={() => first} />
                <EntryList />
            </AnalysisSectionsProvider>
        );

        expect(screen.getByTestId("order")).toHaveTextContent("first,second");
        document.body.removeChild(host);
    });

    it("drops an entry on unregister", () => {
        const host = document.createElement("div");
        const a = document.createElement("div");
        host.appendChild(a);
        document.body.appendChild(host);

        function Toggle({ show }: { show: boolean }): React.ReactElement | null {
            return show ? <Register id="a" label="A" getEl={() => a} /> : null;
        }

        const { screen, rerender } = rtlRender(
            <AnalysisSectionsProvider>
                <Toggle show />
                <EntryList />
            </AnalysisSectionsProvider>
        );
        expect(screen.getByTestId("order")).toHaveTextContent("a");

        act(() => {
            rerender(
                <AnalysisSectionsProvider>
                    <Toggle show={false} />
                    <EntryList />
                </AnalysisSectionsProvider>
            );
        });
        expect(screen.getByTestId("order")).toHaveTextContent("");
        document.body.removeChild(host);
    });
});
