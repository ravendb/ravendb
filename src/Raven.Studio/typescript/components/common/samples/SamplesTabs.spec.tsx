import React from "react";
import { fireEvent, rtlRender } from "test/rtlTestUtils";
import SamplesTabs from "./SamplesTabs";
import { SamplesTab } from "./partials/samplesTypes";

const tabs: SamplesTab[] = [
    {
        key: "one",
        label: "Tab one",
        icon: "document",
        content: () => <div>one-content</div>,
    },
    {
        key: "two",
        label: "Tab two",
        icon: "indent",
        hasSearch: true,
        searchPlaceholder: "Search two",
        content: ({ search }) => <div>{`two-content:${search}`}</div>,
    },
];

describe("SamplesTabs", () => {
    it("propagates onSelect from tab content", async () => {
        const onSelect = jest.fn();
        const selectableTab: SamplesTab = {
            key: "selectable",
            label: "Selectable",
            icon: "document",
            content: ({ onSelect: select }) => (
                <button type="button" onClick={() => select("loaded-script")}>
                    Load sample
                </button>
            ),
        };

        const { screen, fireClick } = rtlRender(<SamplesTabs tabs={[selectableTab]} onSelect={onSelect} />);

        await fireClick(screen.getByText("Load sample"));
        expect(onSelect).toHaveBeenCalledWith("loaded-script");
    });

    it("renders only provided tabs and shows first tab content", () => {
        const { screen } = rtlRender(<SamplesTabs tabs={[tabs[0]]} onSelect={jest.fn()} />);

        expect(screen.getByText("Tab one")).toBeInTheDocument();
        expect(screen.queryByText("Tab two")).not.toBeInTheDocument();
        expect(screen.getByText("one-content")).toBeInTheDocument();
        expect(screen.queryByPlaceholderText("Search two")).not.toBeInTheDocument();
    });

    it("shows search input only on tabs with hasSearch", async () => {
        const { screen, fireClick } = rtlRender(<SamplesTabs tabs={tabs} onSelect={jest.fn()} />);

        expect(screen.queryByPlaceholderText("Search two")).not.toBeInTheDocument();

        await fireClick(screen.getByText("Tab two"));
        expect(screen.getByPlaceholderText("Search two")).toBeInTheDocument();
    });

    it("persists search value across tab switches", async () => {
        const { screen, fireClick } = rtlRender(<SamplesTabs tabs={tabs} onSelect={jest.fn()} />);

        await fireClick(screen.getByText("Tab two"));

        const input = screen.getByPlaceholderText("Search two");
        fireEvent.change(input, { target: { value: "abc" } });

        expect(screen.getByText("two-content:abc")).toBeInTheDocument();

        await fireClick(screen.getByText("Tab one"));
        await fireClick(screen.getByText("Tab two"));

        expect(screen.getByPlaceholderText("Search two")).toHaveValue("abc");
        expect(screen.getByText("two-content:abc")).toBeInTheDocument();
    });
});
