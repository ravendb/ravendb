import React from "react";
import {
    act, fireEvent,
    getQueriesForElement,
    queries, render,
    RenderOptions,
    screen,
    cleanup
} from "@testing-library/react";
import { mockServices } from "./mocks/MockServices";
import { Screen } from "@testing-library/dom/types/screen";
import { ServiceProvider } from "../components/hooks/useServices";
import * as byNameQueries from "./byNameQueries";
import * as byClassNameQueries from "./byClassNameQueries";

function genericRtlRender(providers: () =>
    (props: { children: any }) => JSX.Element, ui: React.ReactElement, options?: { disableWrappers?: boolean; } & Omit<RenderOptions, "queries">) {

    const { disableWrappers, ...restOptions } = options || {};
    const allQueries = { ...queries, ...byNameQueries, ...byClassNameQueries };
    const container = render(ui, {
        wrapper: disableWrappers ? undefined : providers(),
        queries: allQueries, ...restOptions
    });

    const getQueriesForElementFunc = (element: any) => getQueriesForElement(element, 
        { ...queries, ...byNameQueries, ...byClassNameQueries });
    const localScreen = getQueriesForElementFunc(document.body) as Screen<typeof allQueries>;
    localScreen.logTestingPlaygroundURL = screen.logTestingPlaygroundURL;

    return {
        ...container,
        screen: localScreen,
        fillInput,
        fireClick,
        cleanup,
        getQueriesForElement: getQueriesForElementFunc
    };
}

async function fireClick(element: HTMLElement) {
    expect(element).toBeTruthy();
    return await act(async () => {
        fireEvent.click(element);
    });
}

async function fillInput(element: HTMLElement, value: string) {
    return await act(async () => {
        await fireEvent.change(element, { target: { value } });
    });
}

const AllProviders = () => ({ children }: any) => {
    return (
        <ServiceProvider services={mockServices.context}>
            {children}
        </ServiceProvider>
    );
};

export function rtlRender(ui: React.ReactElement, options?: { disableWrappers?: boolean; initialUrl?: string } & Omit<RenderOptions, "queries">) {
    return genericRtlRender(AllProviders, ui, options);
}

export * from "@testing-library/react";
