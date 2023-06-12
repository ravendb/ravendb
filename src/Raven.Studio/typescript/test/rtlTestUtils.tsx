import React, { useState } from "react";
import {
    act,
    fireEvent,
    getQueriesForElement,
    queries,
    render,
    RenderOptions,
    screen,
    cleanup,
} from "@testing-library/react";
import { mockServices } from "./mocks/services/MockServices";
import { Screen } from "@testing-library/dom/types/screen";
import { configureMockServices, ServiceProvider } from "components/hooks/useServices";
import * as byNameQueries from "./byNameQueries";
import * as byClassNameQueries from "./byClassNameQueries";
import { ChangesProvider } from "hooks/useChanges";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import { createStoreConfiguration } from "components/store";
import { Provider as ReduxProvider } from "react-redux";
import { setEffectiveTestStore } from "components/storeCompat";
import { DirtyFlagProvider } from "components/hooks/useDirtyFlag";

let needsTestMock = true;

if (needsTestMock) {
    configureMockServices(mockServices.context);
    needsTestMock = false;
}

function genericRtlRender(
    providers: () => (props: { children: any }) => JSX.Element,
    ui: React.ReactElement,
    options?: { disableWrappers?: boolean } & Omit<RenderOptions, "queries">
) {
    const { disableWrappers, ...restOptions } = options || {};
    const allQueries = { ...queries, ...byNameQueries, ...byClassNameQueries };
    const container = render(ui, {
        wrapper: disableWrappers ? undefined : providers(),
        queries: allQueries,
        ...restOptions,
    });

    const getQueriesForElementFunc = (element: any) =>
        getQueriesForElement(element, { ...queries, ...byNameQueries, ...byClassNameQueries });
    const localScreen = getQueriesForElementFunc(document.body) as Screen<typeof allQueries>;
    localScreen.logTestingPlaygroundURL = screen.logTestingPlaygroundURL;

    return {
        ...container,
        screen: localScreen,
        fillInput,
        fireClick,
        cleanup,
        getQueriesForElement: getQueriesForElementFunc,
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

const AllProviders = () => AllProvidersInner;

function AllProvidersInner({ children }: any) {
    const [store] = useState(() => createStoreConfiguration());

    setEffectiveTestStore(store);

    return (
        <ReduxProvider store={store}>
            <DirtyFlagProvider setIsDirty={mockHooks.useDirtyFlag.mock}>
                <ServiceProvider services={mockServices.context}>
                    <ChangesProvider changes={mockHooks.useChanges.mock}>{children}</ChangesProvider>
                </ServiceProvider>
            </DirtyFlagProvider>
        </ReduxProvider>
    );
}

export function rtlRender(
    ui: React.ReactElement,
    options?: { disableWrappers?: boolean; initialUrl?: string } & Omit<RenderOptions, "queries">
) {
    return genericRtlRender(AllProviders, ui, options);
}

export * from "@testing-library/react";
